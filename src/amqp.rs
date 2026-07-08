//! Functionality supporting both the `[crate::sources::amqp]` source and `[crate::sinks::amqp]` sink.
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use vector_lib::configurable::configurable_component;

use crate::serde::OneOrMany;

/// AMQP connection options.
#[configurable_component]
#[derive(Clone, Debug)]
pub(crate) struct AmqpConfig {
    /// URI for the AMQP server, or a list of URIs to fail over between.
    ///
    /// The URI has the format of
    /// `amqp://<user>:<password>@<host>:<port>/<vhost>?timeout=<seconds>`.
    ///
    /// The default vhost can be specified by using a value of `%2f`.
    ///
    /// To connect over TLS, a scheme of `amqps` can be specified instead. For example,
    /// `amqps://...`. Additional TLS settings, such as client certificate verification, can be
    /// configured under the `tls` section.
    ///
    /// When multiple URIs are given, servers are tried in a round-robin fashion until a
    /// connection is established, which is then used until the next connection error. Set the
    /// `timeout` parameter on each URI so that unreachable servers do not delay failover.
    #[configurable(metadata(
        docs::examples = "amqp://user:password@127.0.0.1:5672/%2f?timeout=10",
    ))]
    pub(crate) connection_string: OneOrMany<String>,

    #[configurable(derived)]
    pub(crate) tls: Option<crate::tls::TlsConfig>,
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            connection_string: OneOrMany::One("amqp://127.0.0.1/%2f".to_string()),
            tls: None,
        }
    }
}

/// Polls the connection until a connection can be made.
/// Gives up after 5 attempts.
#[cfg(feature = "amqp-integration-tests")]
#[cfg(test)]
pub(crate) async fn await_connection(connection: &AmqpConfig) {
    let mut pause = tokio::time::Duration::from_millis(1);
    let mut attempts = 0;

    loop {
        let connection = connection.clone();
        if connection.connect().await.is_ok() {
            return;
        }
        attempts += 1;

        if attempts == 5 {
            return;
        }

        tokio::time::sleep(pause).await;
        pause *= 2;
    }
}

impl AmqpConfig {
    /// Returns the configured connection URIs.
    pub(crate) fn connection_urls(&self) -> Vec<String> {
        self.connection_string.clone().to_vec()
    }

    /// Connects by trying each configured server in order, returning the first successful
    /// connection.
    pub(crate) async fn connect(
        &self,
    ) -> Result<(lapin::Connection, lapin::Channel), Box<dyn std::error::Error + Send + Sync>> {
        let urls = self.connection_urls();
        if urls.is_empty() {
            return Err("`connection_string` must contain at least one URI.".into());
        }
        let mut last_error = None;
        for url in &urls {
            match self.connect_url(url).await {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    warn!(
                        message = "Failed to connect to AMQP endpoint.",
                        endpoint = %redact_uri(url),
                        error = %error,
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.expect("at least one connection attempt was made"))
    }

    /// Connects to a single AMQP endpoint.
    pub(crate) async fn connect_url(
        &self,
        addr: &str,
    ) -> Result<(lapin::Connection, lapin::Channel), Box<dyn std::error::Error + Send + Sync>> {
        let conn = match &self.tls {
            Some(tls) => {
                let cert_chain = if let Some(ca) = &tls.ca_file {
                    Some(tokio::fs::read_to_string(ca.to_owned()).await?)
                } else {
                    None
                };
                let identity = if let Some(identity) = &tls.key_file {
                    let der = tokio::fs::read(identity.to_owned()).await?;
                    Some(OwnedIdentity::PKCS12 {
                        der,
                        password: tls
                            .key_pass
                            .as_ref()
                            .map(|s| s.to_string())
                            .unwrap_or_else(String::default),
                    })
                } else {
                    None
                };
                let tls_config = OwnedTLSConfig {
                    identity,
                    cert_chain,
                };
                lapin::Connection::connect_with_config(
                    addr,
                    lapin::ConnectionProperties::default(),
                    tls_config,
                    async_rs::Runtime::tokio_current(),
                )
                .await
            }
            None => lapin::Connection::connect(addr, lapin::ConnectionProperties::default()).await,
        }?;
        let channel = conn.create_channel().await?;
        Ok((conn, channel))
    }
}

/// Renders a connection URI for logging, with any password redacted.
pub(crate) fn redact_uri(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(mut url) => {
            if url.password().is_some() {
                _ = url.set_password(Some("***"));
            }
            url.to_string()
        }
        Err(_) => "<unparseable URI>".to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_single_connection_string() {
        let config: AmqpConfig =
            toml::from_str(r#"connection_string = "amqp://user:pass@host1:5672/%2f""#).unwrap();
        assert_eq!(
            config.connection_urls(),
            vec!["amqp://user:pass@host1:5672/%2f"]
        );
    }

    #[test]
    fn parses_multiple_connection_strings() {
        let config: AmqpConfig = toml::from_str(
            r#"connection_string = ["amqp://host1:5672/%2f", "amqp://host2:5672/%2f"]"#,
        )
        .unwrap();
        assert_eq!(
            config.connection_urls(),
            vec!["amqp://host1:5672/%2f", "amqp://host2:5672/%2f"]
        );
    }

    #[tokio::test]
    async fn connect_fails_on_empty_connection_string_list() {
        let config = AmqpConfig {
            connection_string: Vec::new().into(),
            ..Default::default()
        };
        let error = config.connect().await.unwrap_err();
        assert!(error.to_string().contains("at least one URI"));
    }

    #[test]
    fn redacts_password_in_uri() {
        assert_eq!(
            redact_uri("amqp://user:secret@host:5672/%2f?timeout=10"),
            "amqp://user:***@host:5672/%2f?timeout=10"
        );
        assert_eq!(redact_uri("amqp://host:5672/%2f"), "amqp://host:5672/%2f");
        assert_eq!(redact_uri("not a uri"), "<unparseable URI>");
    }
}
