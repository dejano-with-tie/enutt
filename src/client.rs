use std::convert::TryFrom;
use std::net::SocketAddr;

use quinn::{ClientConfig, Endpoint, SendStream};
use tracing::info;

use crate::connection::{ConnectionHolder, ConnectionPool};
use crate::message::Message;
use crate::node::Address;

pub struct Client {
    connection_pool: ConnectionPool,
    endpoint: Endpoint,
}

impl Client {
    pub fn new(cfg: ClientConfig) -> crate::Result<Self> {
        let mut endpoint_builder = Endpoint::builder();
        endpoint_builder.default_client_config(cfg);

        // let OS decide about port
        // TODO: Provide config for local addres and boolean flag allow_random_port
        // use it here instead of hardcoded val
        let (endpoint, _) = endpoint_builder.bind(&"127.0.0.1:0".parse::<SocketAddr>()?)?;

        info!(addr = ?endpoint.local_addr()?, "created endpoint");
        Ok(Self {
            connection_pool: ConnectionPool::new(),
            endpoint,
        })
    }

    pub async fn connect_to(&self, addr: &Address) -> crate::Result<ConnectionHolder> {
        let addr = SocketAddr::try_from(addr).unwrap();

        if let Some((conn, rem)) = self.connection_pool.get(&addr) {
            return ConnectionHolder::new(conn, rem);
        }

        // here we might end up with two concurrent connections being constructed to the same address.
        // this could be handled with tokio's mutex but we don't care because connection pool will
        // allow multiple connections to same addr. See connection id in ConnectionPool struct
        let connecting = self
            .endpoint
            .connect(&addr, crate::config::CERT_DOMAIN_NAME)?;
        let quinn::NewConnection { connection, .. } = connecting.await?;

        let remover = self.connection_pool.insert(addr, connection.clone());

        ConnectionHolder::new(connection, remover)
        // Ok(ConnHo)
    }

    pub async fn send_uni(&self, addr: &Address, message: &Message) -> crate::Result<SendStream> {
        let mut connection = self.connect_to(addr).await?;
        connection.send_uni(message).await
    }
}
