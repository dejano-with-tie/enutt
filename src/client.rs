use std::convert::TryFrom;
use std::net::SocketAddr;

use quinn::{ClientConfig, Endpoint, RecvStream, SendStream};
use tokio::net::UdpSocket;
use tracing::info;

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
}

impl Client {
    pub fn new(cfg: ClientConfig) -> crate::Result<Self> {
        let mut endpoint_builder = Endpoint::builder();
        endpoint_builder.default_client_config(cfg);

        // let OS decide about port
        // TODO: Provide config for local address and boolean flag allow_random_port, use it here instead of hardcoded val
        let (endpoint, _) = endpoint_builder.bind(
            &"127.0.0.1:0"
                .parse::<SocketAddr>()
                .map_err(|e| Error::UnexpectedSrc(Box::new(e)))?,
        )?;

        info!(addr = ?endpoint.local_addr()?, "created endpoint");
        Ok(Self {
            connection_pool: ConnectionPool::new(),
            endpoint,
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
            .connect(&addr, crate::config::CERT_DOMAIN_NAME)?;
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

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::time::Duration;

    use quinn::{ConnectError, ConnectionError, Endpoint};

    use crate::client::Client;
    use crate::config::new_transport_cfg;
    use crate::node::Address;
    use crate::Error;

    #[tokio::test]
    pub async fn connection_timeout() {
        const TIMEOUT_MSEC: Duration = Duration::from_millis(200);
        let cfg = crate::config::client::insecure(new_transport_cfg(200, 1));
        let client = Client::new(cfg).unwrap();

        let invalid_addr = Address("127.0.0.1:1".into());

        let now = std::time::Instant::now();
        let dt = now.elapsed();
        match client.connect_to(&invalid_addr).await {
            Err(Error::Connection(ConnectionError::TimedOut)) => {
                println!("failed with timeout")
            }
            Err(e) => panic!("Unexpected error: {}", e),
            Ok(_) => panic!("Unexpected success"),
        }

        assert!(dt >= TIMEOUT_MSEC && dt <= TIMEOUT_MSEC + Duration::from_millis(10));

        println!("elapsed: {}", dt.as_millis());
    }

    #[tokio::test]
    pub async fn connect_within_timeout() {
        let mut endpoint = Endpoint::builder();
        endpoint.listen(crate::config::server::self_signed().unwrap().0);

        let recv_addr = Address("127.0.0.1:9010".into());
        let _ = endpoint
            .bind(&std::net::SocketAddr::try_from(&recv_addr).unwrap())
            .unwrap();

        const TIMEOUT_MSEC: Duration = Duration::from_millis(200);
        let cfg = crate::config::client::insecure(new_transport_cfg(200, 1));
        let client = Client::new(cfg).unwrap();
        if let Err(e) = client.connect_to(&recv_addr).await {
            panic!("Unexpected error: {}", e)
        }
    }
}
