use std::convert::TryFrom;
use std::net::SocketAddr;

use quinn::{Endpoint, RecvStream, SendStream};
use tracing::{error, info};

use crate::config::{new_transport_cfg, Quic};
use crate::connection::{ConnectionHolder, ConnectionPool};
use crate::message::Message;
use crate::node::Address;
use crate::Error;

pub struct Client {
    connection_pool: ConnectionPool,
    endpoint: Endpoint,
    cert_domain_name: String,
}
/// TODO: Should move this ti Membership::State struct
impl Client {
    pub fn new(quic_cfg: &Quic, cfg: &crate::config::Client) -> crate::Result<Self> {
        let client_cfg = crate::config::client::insecure(new_transport_cfg(
            quic_cfg.timeout,
            quic_cfg.keep_alive,
        ));
        let mut endpoint_builder = Endpoint::builder();
        endpoint_builder.default_client_config(client_cfg);

        let addr = &format!("127.0.0.1:{}", cfg.port)
            .parse::<SocketAddr>()
            .map_err(|e| Error::Configuration(e.to_string()))?;
        let (endpoint, _) = endpoint_builder.bind(addr).map_err(|e| {
            Error::Configuration(format!(
                "client unable to bind to [{}]; source = {}",
                &cfg.port, e
            ))
        })?;

        info!(addr = ?endpoint.local_addr()?, "created endpoint");
        Ok(Self {
            connection_pool: ConnectionPool::new(),
            endpoint,
            cert_domain_name: quic_cfg.cert_domain_name.clone(),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    pub fn close(&self, addr: &Address) {
        let addr = SocketAddr::try_from(addr).unwrap();

        if let Some((conn, rem)) = self.connection_pool.get(&addr) {
            info!("Remove connection {}", addr);
            ConnectionHolder::new(conn, rem).close();
        }
    }

    pub async fn connect_to(&self, addr: &Address) -> crate::Result<ConnectionHolder, Error> {
        let addr = SocketAddr::try_from(addr).unwrap();

        if let Some((conn, rem)) = self.connection_pool.get(&addr) {
            return Ok(ConnectionHolder::new(conn, rem));
        }

        // here we might end up with two concurrent connections being constructed to the same address.
        // this could be handled with tokio's mutex but we don't care because connection pool will
        // allow multiple connections to same addr. See connection id in ConnectionPool struct
        let connecting = self
            .endpoint
            .connect(&addr, self.cert_domain_name.as_str())?;
        let quinn::NewConnection { connection, .. } = connecting.await?;

        let remover = self.connection_pool.insert(addr, connection.clone());

        Ok(ConnectionHolder::new(connection, remover))
    }

    pub async fn send_uni(&self, addr: &Address, message: &Message) -> crate::Result<()> {
        let mut connection = self.connect_to(addr).await?;
        connection.send_uni(message).await
    }

    pub async fn send_bi(
        &self,
        addr: &Address,
        message: &Message,
    ) -> crate::Result<(SendStream, RecvStream)> {
        let mut connection = self.connect_to(addr).await?;
        connection.send_bi(message).await
    }

    pub fn rtt(
        &self,
        addr: &Address,
        default_rtt: &tokio::time::Duration,
    ) -> tokio::time::Duration {
        self.connection_pool
            .get(&SocketAddr::try_from(addr).unwrap())
            .map_or(*default_rtt, |(c, _)| c.rtt())
    }
}
