use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed::Pool;
use lapin::options::ConfirmSelectOptions;

use super::{config::AmqpSinkConfig, service::AmqpError};
use crate::amqp::{AmqpConfig, redact_uri};

pub type AmqpSinkChannels = Pool<AmqpSinkChannelManager>;

pub(super) fn new_channel_pool(config: &AmqpSinkConfig) -> crate::Result<AmqpSinkChannels> {
    let max_channels = config.max_channels.try_into().map_err(|_| {
        Box::new(AmqpError::PoolError {
            error: "max_channels must fit into usize".into(),
        })
    })?;
    if max_channels == 0 {
        return Err(Box::new(AmqpError::PoolError {
            error: "max_channels must be positive".into(),
        }));
    }
    let channel_manager = AmqpSinkChannelManager::new(&config.connection)?;
    let channels = Pool::builder(channel_manager)
        .max_size(max_channels)
        .runtime(deadpool::Runtime::Tokio1)
        .build()?;
    debug!("AMQP channel pool created with max size: {}", max_channels);
    Ok(channels)
}

/// A channel pool manager for the AMQP sink.
/// This manager is responsible for creating and recycling AMQP channels.
/// It uses the `deadpool` crate to manage the channels.
///
/// New connections stick to one of the configured servers until a connection error is
/// detected, at which point subsequent connections fail over to the remaining servers in a
/// round-robin fashion.
pub(crate) struct AmqpSinkChannelManager {
    config: AmqpConfig,
    urls: Vec<String>,
    next_index: AtomicUsize,
}

impl deadpool::managed::Manager for AmqpSinkChannelManager {
    type Type = lapin::Channel;
    type Error = AmqpError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let count = self.urls.len();
        let start = self.next_index.load(Ordering::Relaxed);
        let mut last_error = None;
        for attempt in 0..count {
            let index = (start + attempt) % count;
            let url = &self.urls[index];
            match self.new_channel(url).await {
                Ok(channel) => {
                    self.next_index.store(index, Ordering::Relaxed);
                    info!(
                        message = "Created a new channel to the AMQP broker.",
                        id = channel.id(),
                        endpoint = %redact_uri(url),
                    );
                    return Ok(channel);
                }
                Err(error) => {
                    warn!(
                        message = "Failed to connect to AMQP endpoint.",
                        endpoint = %redact_uri(url),
                        attempts_remaining = count - attempt - 1,
                        error = %error,
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.expect("connection URL list cannot be empty"))
    }

    async fn recycle(
        &self,
        channel: &mut Self::Type,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        let status = channel.status();
        if status.connected() {
            Ok(())
        } else {
            // The channel lost its connection: start the next connection sweep at the
            // following server instead of re-dialing the one that just failed.
            self.next_index.fetch_add(1, Ordering::Relaxed);
            Err((AmqpError::ChannelClosed {
                status: status.clone(),
            })
            .into())
        }
    }
}

impl AmqpSinkChannelManager {
    /// Creates a new channel pool manager for the AMQP sink.
    pub fn new(config: &AmqpConfig) -> crate::Result<Self> {
        let urls = config.connection_urls();
        if urls.is_empty() {
            return Err(Box::new(AmqpError::PoolError {
                error: "`connection_string` must contain at least one URI".into(),
            }));
        }
        Ok(Self {
            config: config.clone(),
            urls,
            next_index: AtomicUsize::new(0),
        })
    }

    /// Creates a new AMQP channel to the given endpoint.
    async fn new_channel(&self, url: &str) -> Result<lapin::Channel, AmqpError> {
        let (_, channel) = self
            .config
            .connect_url(url)
            .await
            .map_err(|e| AmqpError::ConnectFailed { error: e })?;

        // Enable confirmations on the channel.
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| AmqpError::ConnectFailed { error: Box::new(e) })?;

        Ok(channel)
    }
}
