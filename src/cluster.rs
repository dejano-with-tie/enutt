use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::err;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::{Future, FutureExt};
use parking_lot::{Mutex, RawMutex};
use quinn::{Endpoint, NewConnection, RecvStream, SendStream};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, info_span, instrument, warn};
use tracing_futures::Instrument;
use uuid::Uuid;

use crate::config::{Config, ConfigBuilder};
use crate::connection::Connection;
use crate::message::{Message, Multicast};
use crate::node::{Address, Peer};
use crate::{Error, ErrorKind, Id};

use super::node::Node;
use super::Result;

pub struct Cluster {
    context: Arc<Context>,
    client_cfg: quinn::ClientConfig,
    server_cfg: quinn::ServerConfig,
}

/// Holds state
pub struct Context {
    config: Config,
    node: Arc<Node>,
    gossip_tx: UnboundedSender<Multicast>,
}

impl Context {
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn gossip_tx(&self) -> &UnboundedSender<Multicast> {
        &self.gossip_tx
    }

    // TODO: Clean this up with ENTRY thing
    pub fn add_peer(&self, peer: Peer) -> std::result::Result<Peer, ErrorKind> {
        let rd = self.node.peers().read();
        if rd.get_key_value(&peer.id()).is_some() {
            return Err(ErrorKind::KnownMember(peer));
        }
        drop(rd);

        let mut wr = self.node.peers().write();
        if wr.insert(*peer.id(), peer.inner().clone()).is_some() {
            return Err(ErrorKind::Unexpected(
                "context error; unable to add new peer".into(),
            ));
        }

        drop(wr);

        info!("new peer: {}", peer = peer);
        Ok(Peer::from(
            self.node()
                .peers()
                .read()
                .get_key_value(&peer.id())
                .unwrap(),
        ))
    }
}

impl Cluster {
    pub async fn new(config: Config) -> Result<Self> {
        // discover who I am
        let address = Address(format!("127.0.0.1:{}", config.app_port()));
        let node = Arc::new(Node::new(Id::default(), address, HashMap::new()));

        // discover nat capabilities

        // configure client
        let client_cfg = crate::config::client::insecure();

        // configure server
        let (server_cfg, _cert) = crate::config::server::self_signed()?;

        // gossip _task_
        let gossip_tx = crate::gossip::run(Arc::clone(&node));

        // configure context
        let context = Context {
            config,
            gossip_tx,
            node: Arc::clone(&node),
        };

        // return api which is exposing send bi/uni/udp messages
        Ok(Self {
            context: Arc::new(context),
            client_cfg,
            server_cfg,
        })
    }

    pub async fn bootstrap(&self) -> Result<(), ErrorKind> {
        // setup nat _task_, UPnP

        // setup server _task_
        let ctx = Arc::clone(&self.context);
        crate::server::run(ctx, self.server_cfg.clone())
            .await
            .map_err(|e| {
                error!("failed: {reason}", reason = e);
                ErrorKind::BootstrapFailure
            })?;

        // join
        self.join().await?;

        // let node = self.context().node();
        // let peers = node.peers().read();
        // println!("{} -> {:?}", node.address(), peers);

        Ok(())
    }

    async fn join(&self) -> crate::Result<(), crate::ErrorKind> {
        let bootstrap_nodes = self.context.config.network().bootstrap_nodes();
        let (mut connection, _bootstrap_node) = match self.connect_any(bootstrap_nodes).await? {
            None => return Ok(()),
            Some(connection) => connection,
        };

        // Send _join_ message and update members
        let message = Message::Join(Peer::from(self.context.node.as_ref()));
        let (_, mut recv_stream) = connection
            .send_bi(&message)
            .await
            .map_err(|_e| ErrorKind::BootstrapFailure)?;

        let response = Message::read(&mut recv_stream)
            .await
            .map_err(|e| {
                error!("failed to join cluster: {reason}", reason = e);
                ErrorKind::BootstrapFailure
            })?
            .unwrap();

        return match response {
            Message::Membership(peers) => {
                peers
                    .into_iter()
                    .for_each(|peer| match self.context.add_peer(peer) {
                        Err(err) => warn!("failed: {:?}", err),
                        Ok(_) => {}
                    });

                Ok(())
            }
            _ => {
                error!("failed to join cluster; bootstrap node responded with wrong message type");
                return Err(ErrorKind::BootstrapFailure);
            }
        };

        // let message = Message::Join("hello my friend".into());
        // match message.write(&mut send_stream).await {
        //     Ok(_) => {}
        //     Err(crate::message::Error::StreamWrite(quinn::WriteError::Stopped(e))) => {
        //         eprintln!("failed to send message; stream is closed; {}", e);
        //         // TODO: Return indication that stream is closed
        //     }
        //     Err(e) => eprintln!("failed to send message; {}", e),
        // }
        // send_stream.write_all(b"will this fail?").await?;
    }

    async fn connect_any(
        &self,
        bootstrap_nodes: &[Address],
    ) -> Result<Option<(Connection, Address)>, crate::ErrorKind> {
        if bootstrap_nodes.is_empty() {
            return Err(ErrorKind::Configuration(
                "no known nodes; unable to bootstrap".into(),
            ));
        }

        let mut connection = Connection::new(self.client_cfg.clone()).map_err(|e| {
            error!("failed: {reason}", reason = e);
            ErrorKind::Unexpected("unable to create client connection".into())
        })?;

        // Attempt to connect to all nodes and return the first one to succeed
        let mut bootstrap_nodes = bootstrap_nodes
            .iter()
            .filter(|addr| addr != &self.context.node.address())
            .take(1) // TODO: Only take first for now
            .peekable();

        if bootstrap_nodes.peek().is_none() {
            warn!("no known nodes; unable to join cluster, starting on my own");
            return Ok(None);
        }

        let bootstrap_node = bootstrap_nodes.next().unwrap();
        if let Err(e) = connection.connect_to(bootstrap_node).await {
            error!("unable to connect to bootstrap node: {reason}", reason = e);
            return Ok(None);
        }

        Ok(Some((connection, bootstrap_node.clone())))
    }

    pub fn context(&self) -> &Arc<Context> {
        // cfg!(test)
        &self.context
    }
}

async fn create_conn(address: &Address) -> Result<(SendStream, RecvStream)> {
    let client_cfg = crate::config::client::insecure();
    let mut endpoint_builder = Endpoint::builder();
    endpoint_builder.default_client_config(client_cfg);

    // Bind this endpoint to a UDP socket on the given client address.
    // let OS decide about port
    // let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let (endpoint, _) = endpoint_builder.bind(&"127.0.0.1:0".parse::<SocketAddr>()?)?;

    // Connect to the server passing in the server name which is supposed to be in the server certificate.
    let NewConnection { connection, .. } = endpoint
        .connect(
            &SocketAddr::try_from(address)?,
            crate::config::server::CERT_DOMAIN_NAME,
        )?
        .await?;
    info!(addr = ?address, "created endpoint");
    Ok(connection.open_bi().await?)
}
