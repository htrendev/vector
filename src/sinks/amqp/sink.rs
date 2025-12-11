//! The sink for the `AMQP` sink that wires together the main stream that takes the
//! event and sends it to `AMQP`.
use lapin::BasicProperties;
use serde::Serialize;
use tower::ServiceBuilder;

use super::{
    BuildError,
    config::{AmqpPropertiesConfig, AmqpSinkConfig},
    encoder::AmqpEncoder,
    request_builder::AmqpRequestBuilder,
    service::{AmqpRetryLogic, AmqpService},
};
use crate::sinks::{
    prelude::*,
    util::{
        service::{ServiceBuilderExt, Svc},
    },
};

/// Stores the event together with the rendered exchange and routing_key values.
/// This is passed into the `RequestBuilder` which then splits it out into the event
/// and metadata containing the exchange and routing_key.
/// This event needs to be created prior to building the request so we can filter out
/// any events that error whilst rendering the templates.
#[derive(Serialize)]
pub(super) struct AmqpEvent {
    pub(super) event: Event,
    pub(super) exchange: String,
    pub(super) routing_key: String,
    pub(super) properties: BasicProperties,
}

/// Transforms an event into an `AMQP` event by rendering the required template fields.
/// Returns None if there is an error whilst rendering.
fn make_amqp_event(
    event: Event,
    exchange: &Template,
    routing_key: &Option<Template>,
    properties: &Option<AmqpPropertiesConfig>,
) -> Option<AmqpEvent> {
    let exchange = exchange
        .render_string(&event)
        .map_err(|missing_keys| {
            emit!(TemplateRenderingError {
                error: missing_keys,
                field: Some("exchange"),
                drop_event: true,
            })
        })
        .ok()?;

    let routing_key = match routing_key {
        None => String::new(),
        Some(key) => key
            .render_string(&event)
            .map_err(|missing_keys| {
                emit!(TemplateRenderingError {
                    error: missing_keys,
                    field: Some("routing_key"),
                    drop_event: true,
                })
            })
            .ok()?,
    };

    let properties = match properties {
        None => BasicProperties::default(),
        Some(prop) => prop.build(&event)?,
    };

    Some(AmqpEvent {
        event,
        exchange,
        routing_key,
        properties,
    })
}

pub(super) struct AmqpSink {
    service: Svc<AmqpService, AmqpRetryLogic>,
    exchange: Template,
    routing_key: Option<Template>,
    properties: Option<AmqpPropertiesConfig>,
    transformer: Transformer,
    encoder: crate::codecs::Encoder<()>,
}

impl AmqpSink {
    pub(super) async fn new(config: AmqpSinkConfig) -> crate::Result<Self> {
        let channels = super::channel::new_channel_pool(&config)
            .map_err(|e| BuildError::AmqpCreateFailed { source: e })?;

        let transformer = config.encoding.transformer();
        let serializer = config.encoding.build()?;
        let encoder = crate::codecs::Encoder::<()>::new(serializer);

        let request_settings = config.request.into_settings();
        let service = ServiceBuilder::new()
            .settings(request_settings, AmqpRetryLogic)
            .service(AmqpService { channels });

        Ok(AmqpSink {
            service,
            exchange: config.exchange,
            routing_key: config.routing_key,
            properties: config.properties,
            transformer,
            encoder,
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let request_builder = AmqpRequestBuilder {
            encoder: AmqpEncoder {
                encoder: self.encoder.clone(),
                transformer: self.transformer.clone(),
            },
        };

        // Move the fields we need out of self before the closure borrows it
        let exchange = self.exchange.clone();
        let routing_key = self.routing_key.clone();
        let properties = self.properties.clone();

        input
            .filter_map(move |event| {
                let exchange = exchange.clone();
                let routing_key = routing_key.clone();
                let properties = properties.clone();

                std::future::ready(make_amqp_event(
                    event,
                    &exchange,
                    &routing_key,
                    &properties,
                ))
            })
            .request_builder(default_request_builder_concurrency_limit(), request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        error!("Failed to build AMQP request: {:?}.", e);
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .protocol("amqp_0_9_1")
            .run()
            .await
    }
}

#[async_trait]
impl StreamSink<Event> for AmqpSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
