use std::sync::Arc;

use futures::{Future, FutureExt};
use parking_lot::{Mutex, RawMutex};
use quinn::{Endpoint, NewConnection, RecvStream, SendStream};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tracing::{error, info, info_span, instrument, warn};

use crate::client::Client;
use crate::config::{Config, ConfigBuilder};
use crate::connection::ConnectionHolder;
use crate::gossip::GossipQueue;
use crate::membership::Membership;
use crate::message::{Message, Multicast};
use crate::node::{Address, Peer, PeerInner};
use crate::{Error, ErrorKind, Id};

use super::node::Node;
use super::Result;

pub struct Cluster {
    context: Arc<Context>,
    client_cfg: quinn::ClientConfig,
    server_cfg: quinn::ServerConfig,
    shutdown: (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<Result<(), ErrorKind>>,
    ),
}

/// Holds shared state
/// TODO: Maybe rename this to shared?
pub struct Context {
    membership: Arc<Membership>,
    config: Config,
    // TODO: Do we actually need Membership struct here?
    node: Arc<Node>,
    /// every part of app should be able to gossip
    gossip: GossipQueue,
    /// every part of app should be able to communicate over quic
    client: Arc<Client>,
}

impl Context {
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn gossip(&self) -> &GossipQueue {
        &self.gossip
    }

    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }
    pub fn membership(&self) -> &Arc<Membership> {
        &self.membership
    }
}

impl Cluster {
    pub async fn new(config: Config) -> Result<Self> {
        // configure client
        let client_cfg = crate::config::client::insecure();

        // configure server
        let (server_cfg, _cert) = crate::config::server::self_signed()?;

        // client
        // TODO: Handle parsing of bootstrap ip/port in one place
        let client = Arc::new(Client::new(client_cfg.clone())?);

        // discover who I am
        let address = Address(format!("127.0.0.1:{}", config.app_port()));
        let node = Arc::new(Node::new(Id::default(), address));
        let membership = Arc::new(Membership::new(&node.id(), node.address()));

        // discover nat capabilities

        // gossip _task_
        let gossip = crate::gossip::run(
            config.gossip(),
            Arc::clone(&client),
            Arc::clone(&node),
            Arc::clone(&membership),
        );

        // configure context
        let context = Arc::new(Context {
            membership: Arc::clone(&membership),
            config,
            gossip,
            node: Arc::clone(&node),
            client: Arc::clone(&client),
        });

        // shutdown
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_result = crate::shutdown::listen(Arc::clone(&context), shutdown_rx).await;

        // return api which is exposing send bi/uni/udp messages
        Ok(Self {
            context: Arc::clone(&context),
            client_cfg,
            server_cfg,
            shutdown: (shutdown_tx, shutdown_result),
        })
    }

    pub async fn shutdown(self) -> Result<(), ErrorKind> {
        info!("Shutting down gracefully");

        self.shutdown.0.send(()).unwrap();

        match self.shutdown.1.await.unwrap() {
            Ok(_) => {
                info!("successful shutdown");
                Ok(())
            }
            Err(e) => {
                error!(
                    "failed to shutdown gracefully; reason: {reason}",
                    reason = e
                );
                Err(ErrorKind::BootstrapFailure)
            }
        }
    }

    /// 1. Run server task
    /// 2. Join cluster
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
        let (_, mut recv_stream) = connection
            .send_bi(&Message::Join(Peer::from(self.context.node.as_ref())))
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
                peers.into_iter().for_each(|peer| {
                    if let Err(err) = self.context.membership.add(peer) {
                        warn!("failed: {:?}", err)
                    }
                });
                Ok(())
            }
            _ => {
                error!("failed to join cluster; bootstrap node responded with wrong message type");
                return Err(ErrorKind::BootstrapFailure);
            }
        };
    }

    async fn connect_any<'a>(
        &self,
        bootstrap_nodes: &'a [Address],
    ) -> Result<Option<(ConnectionHolder, &'a Address)>, crate::ErrorKind> {
        if bootstrap_nodes.is_empty() {
            return Err(ErrorKind::Configuration(
                "no known nodes; unable to bootstrap".into(),
            ));
        }

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
        match self.context.client.connect_to(bootstrap_node).await {
            Err(e) => {
                error!("unable to connect to bootstrap node: {reason}", reason = e);
                Ok(None)
            }
            Ok(conn) => Ok(Some((conn, bootstrap_node))),
        }
    }

    pub fn context(&self) -> &Arc<Context> {
        // cfg!(test)
        &self.context
    }
}
