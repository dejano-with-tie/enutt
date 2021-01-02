use std::convert::TryFrom;
use std::net::SocketAddr;

use quinn::{ClientConfig, Endpoint, RecvStream, SendStream};
use tokio::net::UdpSocket;
use tracing::info;

use crate::config::{new_transport_cfg, Quic};
use crate::connection::{ConnectionHolder, ConnectionPool};
use crate::message::Message;
use crate::node::Address;
use crate::Error;

enum Endp {
    RegularTtl,
    ShortTtl,
}

pub struct Client {
    connection_pool: ConnectionPool,
    endpoint: Endpoint,
    cert_domain_name: String,
}

impl Client {
    pub fn new(quic_cfg: &crate::config::Quic, cfg: &crate::config::Client) -> crate::Result<Self> {
        let client_cfg = crate::config::client::insecure(new_transport_cfg(
            quic_cfg.timeout,
            quic_cfg.keep_alive,
        ));
        let mut endpoint_builder = Endpoint::builder();
        endpoint_builder.default_client_config(client_cfg);

        let port = match cfg.port_random {
            true => 0,
            false => cfg.port,
        };
        let (endpoint, _) = endpoint_builder.bind(
            &format!("127.0.0.1:{}", port)
                .parse::<SocketAddr>()
                .map_err(|e| Error::UnexpectedSrc(Box::new(e)))?,
        )?;

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
}
