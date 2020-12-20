use std::convert::TryFrom;
use std::net::SocketAddr;

use quinn::{ClientConfig, Endpoint, RecvStream, SendStream};
use tracing::{debug, info, instrument, warn};
use tracing_futures::Instrument as _;

use crate::message::Message;
use crate::node::Address;

#[derive(Clone)]
pub struct Connection {
    endpoint: Endpoint,
    connection: Option<quinn::Connection>,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    #[instrument(name = "client-connection", skip(cfg))]
    pub fn new(cfg: ClientConfig) -> crate::Result<Self> {
        let mut endpoint_builder = Endpoint::builder();
        endpoint_builder.default_client_config(cfg);

        // Bind this endpoint to a UDP socket on the given client address.
        // let OS decide about port
        // let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let (endpoint, _) = endpoint_builder.bind(&"127.0.0.1:0".parse::<SocketAddr>()?)?;

        debug!(addr = ?endpoint.local_addr()?, "created endpoint");

        Ok(Self {
            endpoint,
            connection: None,
        })
    }

    #[instrument(name = "client-connection", skip(self))]
    pub async fn connect_to(&mut self, peer: &Address) -> crate::Result<()> {
        let connecting = self.endpoint.connect(
            &SocketAddr::try_from(peer)?,
            crate::config::server::CERT_DOMAIN_NAME,
        )?;

        let quinn::NewConnection { connection, .. } = connecting.await?;

        self.connection = Some(connection);

        Ok(())
    }

    pub async fn send_bi(&mut self, message: &Message) -> crate::Result<(SendStream, RecvStream)> {
        // we called connect_to before this so it is alright to unwrap
        let (mut send, recv) = self.connection.as_mut().unwrap().open_bi().await?;

        message.write(&mut send).await?;

        if send.finish().await.is_err() {
            warn!("failed to finish stream gracefully");
        };

        // self.endpoint.wait_idle().await;

        Ok((send, recv))
    }
    pub async fn send_uni(address: &Address, message: &Message) -> crate::Result<SendStream> {
        let mut connection = Connection::new(crate::config::client::insecure()).unwrap();
        connection.connect_to(address).await?;
        // we called connect_to before this so it is alright to unwrap
        let mut send = connection.connection.unwrap().open_uni().await?;

        message.write(&mut send).await?;

        if send.finish().await.is_err() {
            warn!("failed to finish stream gracefully");
        };

        // self.endpoint.wait_idle().await;

        Ok(send)
    }
}
