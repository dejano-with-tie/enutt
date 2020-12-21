use config::FileFormat;
use serde::Deserialize;

use crate::node::Address;
use crate::ErrorKind;
use std::time::Duration;

pub static CERT_DOMAIN_NAME: &str = "enutt";

fn new_transport_cfg(
    idle_timeout_msec: u64,
    keep_alive_interval_msec: u32,
) -> quinn::TransportConfig {
    let mut transport_config = quinn::TransportConfig::default();
    let _ = transport_config
        .max_idle_timeout(Some(Duration::from_millis(idle_timeout_msec)))
        .map_err(|e| ErrorKind::Configuration(e.to_string()))
        .unwrap_or(&mut Default::default());
    let _ = transport_config
        .keep_alive_interval(Some(Duration::from_millis(keep_alive_interval_msec.into())));
    transport_config
}

pub mod client {
    use std::sync::Arc;

    use crate::config::new_transport_cfg;
    use quinn::ClientConfig;

    /// Dummy certificate verifier that treats any certificate as valid.
    pub(crate) struct SkipServerVerification;

    impl rustls::ServerCertVerifier for SkipServerVerification {
        fn verify_server_cert(
            &self,
            _roots: &rustls::RootCertStore,
            _presented_certs: &[rustls::Certificate],
            _dns_name: webpki::DNSNameRef,
            _ocsp_response: &[u8],
        ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
            Ok(rustls::ServerCertVerified::assertion())
        }
    }

    /// Configure client to trust any certificate
    pub fn insecure() -> ClientConfig {
        let mut cfg = quinn::ClientConfigBuilder::default().build();
        cfg.transport = Arc::new(new_transport_cfg(30_000, 10_000));

        // Get a mutable reference to the 'crypto' config in the 'client config'..
        let tls_cfg: &mut rustls::ClientConfig = Arc::get_mut(&mut cfg.crypto).unwrap();

        // Change the certification verifier.
        // This is only available when compiled with 'dangerous_configuration' feature.
        tls_cfg
            .dangerous()
            .set_certificate_verifier(Arc::new(SkipServerVerification));
        cfg
    }
}

pub mod server {
    use quinn::{Certificate, CertificateChain, PrivateKey, ServerConfig, ServerConfigBuilder};

    use crate::config::{new_transport_cfg, CERT_DOMAIN_NAME};
    use std::sync::Arc;

    /// Returns default server configuration along with its certificate.
    pub fn self_signed() -> crate::Result<(ServerConfig, Vec<u8>)> {
        let cert = rcgen::generate_simple_self_signed(vec![CERT_DOMAIN_NAME.into()]).unwrap();
        let cert_der = cert.serialize_der().unwrap();
        let priv_key = cert.serialize_private_key_der();

        let mut config = ServerConfig::default();
        config.transport = Arc::new(new_transport_cfg(30_000, 10_000));

        let mut cfg_builder = ServerConfigBuilder::new(config);

        cfg_builder.certificate(
            CertificateChain::from_certs(vec![Certificate::from_der(&cert_der)?]),
            PrivateKey::from_der(&priv_key)?,
        )?;

        Ok((cfg_builder.build(), cert_der))
    }
}

/// File configuration ([config.yaml]) is deserialized into this struct.
/// These are config details for the application
#[derive(Deserialize)]
pub struct Config {
    app_port: u16,
    network: Network,
    gossip: Gossip,
}

impl Config {
    pub fn app_port(&self) -> u16 {
        self.app_port
    }
    pub fn network(&self) -> &Network {
        &self.network
    }
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }
}

#[derive(Deserialize)]
pub struct Network {
    bootstrap_nodes: Vec<Address>,
}

impl Network {
    pub fn bootstrap_nodes(&self) -> &Vec<Address> {
        &self.bootstrap_nodes
    }
}

#[derive(Deserialize)]
pub struct Gossip {
    fanout: usize,
    frequency: u64,
    rate: usize,
}

impl Gossip {
    pub fn fanout(&self) -> usize {
        self.fanout
    }
    pub fn frequency(&self) -> u64 {
        self.frequency
    }
    pub fn rate(&self) -> usize {
        self.rate
    }
}

/// Config builder
/// Provide setters for optional fields
pub struct ConfigBuilder {
    port: Option<u16>,
    bootstrap_peers: Option<Vec<String>>,
}

impl ConfigBuilder {
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    pub fn bootstrap_peers(&mut self, boot_peers: Vec<String>) -> &mut Self {
        self.bootstrap_peers = Some(boot_peers);
        self
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            port: None,
            bootstrap_peers: None,
        }
    }
}

impl ConfigBuilder {
    pub fn finish(&self) -> Result<Config, config::ConfigError> {
        let mut s = config::Config::default();

        s.merge(config::File::with_name("config").format(FileFormat::Yaml))?
            .merge(config::Environment::with_prefix("app"))?;

        if let Some(port) = self.port {
            s.set("app_port", port as i64).unwrap();
        }

        // println!("{:?}", s.get_table("network"));
        if let Some(ref boot_peers) = self.bootstrap_peers {
            s.set("network.bootstrap_nodes", boot_peers.clone())?;
        }

        let config: Result<Config, config::ConfigError> = s.try_into();

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn when_default_read_from_file() {
        let config = ConfigBuilder::default().finish().unwrap();

        assert_eq!(8081 as u16, config.app_port());
        assert_eq!(
            &vec![Address("127.0.0.1:8081".into())],
            config.network().bootstrap_nodes()
        );
    }

    #[test]
    fn change_port_with_env_var() {
        let new_port = "123";
        std::env::set_var("app_app_port", new_port);
        let config = ConfigBuilder::default().finish().unwrap();

        assert_eq!(123 as u16, config.app_port());
        assert_eq!(
            &vec![Address("127.0.0.1:8081".into())],
            config.network().bootstrap_nodes()
        );
    }

    #[test]
    fn builder() {
        let changed_port = ConfigBuilder::default().port(444).finish().unwrap();
        assert_eq!(444 as u16, changed_port.app_port());
        assert_eq!(
            &vec![Address("127.0.0.1:8081".into())],
            changed_port.network().bootstrap_nodes()
        );

        let replaced_boot_peers = ConfigBuilder::default()
            .bootstrap_peers(vec!["192.168.0.1:8081".into()])
            .finish()
            .unwrap();
        assert_eq!(8081 as u16, replaced_boot_peers.app_port());
        assert_eq!(
            &vec![Address("192.168.0.1:8081".into())],
            replaced_boot_peers.network().bootstrap_nodes()
        );

        let full_builder = ConfigBuilder::default()
            .port(4141)
            .bootstrap_peers(vec!["192.168.0.2:8081".into()])
            .finish()
            .unwrap();
        assert_eq!(4141 as u16, full_builder.app_port());
        assert_eq!(
            &vec![Address("192.168.0.2:8081".into())],
            full_builder.network().bootstrap_nodes()
        );
    }
}
