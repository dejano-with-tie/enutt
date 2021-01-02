use std::sync::Arc;

use futures::{Future, FutureExt};
use parking_lot::{Mutex, RawMutex};
use quinn::{Endpoint, NewConnection, RecvStream, SendStream};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::{watch, Notify};
use tracing::{error, info, info_span, instrument, warn};

use crate::client::Client;
use crate::config::{new_transport_cfg, Config};
use crate::connection::ConnectionHolder;
use crate::gossip::DisseminationQueue;
use crate::membership::{Membership, Peer};
use crate::message::Message;
use crate::node::Address;
use crate::{Error, Id};

use super::node::Node;
use super::Result;

pub struct Cluster {
    context: Arc<Shared>,
    client_cfg: quinn::ClientConfig,
    server_cfg: quinn::ServerConfig,
    shutdown_notify: Arc<Notify>,
}

/// Holds shared state
/// TODO: Improvement: Context/Shared should be Arc and everything inside without Arc
/// Reasoning: Will have to maintain only one atomic reference for Arc<Context>
pub struct Shared {
    membership: Arc<Membership>,
    config: Config,
    /// every part of app should be able to gossip
    gossip: DisseminationQueue,
    /// every part of app should be able to communicate over quic
    client: Arc<Client>,
}

impl Shared {
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn gossip(&self) -> &DisseminationQueue {
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
        let client_cfg = crate::config::client::insecure(new_transport_cfg(30_000, 10_000));

        // configure server
        let (server_cfg, _cert) = crate::config::server::self_signed()?;

        // client
        // TODO: Handle parsing of bootstrap ip/port in one place
        let client = Arc::new(Client::new(client_cfg.clone())?);

        // discover who I am
        let address = Address(format!("127.0.0.1:{}", config.app_port()));
        let node = Node::new(Id::default(), address);
        let membership = Membership::new(node, &config.gossip());

        // discover nat capabilities

        // gossip _task_
        let gossip = crate::gossip::run(
            config.gossip(),
            Arc::clone(&client),
            Arc::clone(&membership),
        );

        // configure context
        let context = Arc::new(Shared {
            membership: Arc::clone(&membership),
            config,
            gossip,
            client: Arc::clone(&client),
        });

        // swim _task_
        let _ = crate::membership::swim::run(context.config.swim(), context.clone());

        // shutdown
        let notify = Arc::new(tokio::sync::Notify::new());

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        crate::shutdown::listen(Arc::clone(&context), notify.clone()).await;

        // return api which is exposing send bi/uni/udp messages
        let cluster = Self {
            context: Arc::clone(&context),
            client_cfg,
            server_cfg,
            shutdown_notify: notify,
        };

        Ok(cluster)
    }

    pub fn shutdown(self) {
        info!("Shutting down gracefully");

        self.shutdown_notify.notify();
    }

    /// 1. Run server task
    /// 2. Join cluster
    pub async fn bootstrap(&self) -> Result<(), Error> {
        // setup nat _task_, UPnP

        // setup server _task_
        let ctx = Arc::clone(&self.context);
        crate::server::run(ctx, self.server_cfg.clone())
            .await
            .map_err(|e| {
                error!("failed: {reason}", reason = e);
                Error::BootstrapFailure
            })?;

        // join
        self.join().await?;

        // let node = self.context().node();
        // let peers = node.peers().read();
        // println!("{} -> {:?}", node.address(), peers);

        Ok(())
    }

    async fn join(&self) -> crate::Result<(), crate::Error> {
        let bootstrap_nodes = self.context.config.network().bootstrap_nodes();
        let (mut connection, _bootstrap_node) = match self.connect_any(bootstrap_nodes).await? {
            None => return Ok(()),
            Some(connection) => connection,
        };

        // Send _join_ message and update members
        let (_, mut recv_stream) = connection
            .send_bi(&Message::Join(Peer::from(self.context.membership.me())))
            .await
            .map_err(|_e| Error::BootstrapFailure)?;

        let response = Message::read(&mut recv_stream)
            .await
            .map_err(|e| {
                error!("failed to join cluster: {reason}", reason = e);
                Error::BootstrapFailure
            })?
            .unwrap();

        return match response {
            Message::Membership(peers) => {
                peers.into_iter().for_each(|peer| {
                    self.context.membership.insert(peer);
                });
                Ok(())
            }
            _ => {
                error!("failed to join cluster; bootstrap node responded with wrong message type");
                return Err(Error::BootstrapFailure);
            }
        };
    }

    async fn connect_any<'a>(
        &self,
        bootstrap_nodes: &'a [Address],
    ) -> Result<Option<(ConnectionHolder, &'a Address)>, crate::Error> {
        if bootstrap_nodes.is_empty() {
            return Err(Error::Configuration(
                "no known nodes; unable to bootstrap".into(),
            ));
        }

        // Attempt to connect to all nodes and return the first one to succeed
        let mut bootstrap_nodes = bootstrap_nodes
            .iter()
            .filter(|addr| addr != &self.context.membership.me().address())
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

    pub fn context(&self) -> &Arc<Shared> {
        // cfg!(test)
        &self.context
    }
}
