use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast::Sender;
use tracing::{error, info_span, instrument, warn};

use crate::client::Client;
use crate::config::Config;
use crate::connection::ConnectionHolder;
use crate::gossip::DisseminationQueue;
use crate::membership::{Membership, Peer, PeerId, PeerInner};
use crate::message::{Message, Multicast};
use crate::node::{Address, NodeId};
use crate::{Error, Id};

use super::node::Node;
use super::Result;

pub struct Cluster {
    shared: Arc<Shared>,
    server_cfg: quinn::ServerConfig,
    shutdown_tx: Sender<()>,
}

/// Holds shared state
/// TODO: Improvement: Context/Shared should be Arc and everything inside without Arc
/// Reasoning: Will have to maintain only one atomic reference for Arc<Context>
pub struct Shared {
    /// every part of app should be able to communicate over quic
    client: Arc<Client>,
    membership: Arc<Membership>,
    config: Config,
    /// every part of app should be able to gossip
    gossip: DisseminationQueue,
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

#[derive(Serialize, Deserialize, Debug)]
struct NetworkSettings {
    node_id: NodeId,
}

macro_rules! settings_file_path {
    () => {
        "{}/.cluster-{}.json"
    };
}
async fn load_settings(cfg: &Config) -> Result<NetworkSettings> {
    let mut file = File::open(Path::new(
        format!(settings_file_path!(), cfg.config_dir_path(), cfg.name()).as_str(),
    ))
    .await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;

    let data: NetworkSettings = serde_json::from_str(&contents).map_err(|e| Error::Io(e.into()))?;

    Ok(data)
}

async fn persist_settings(cfg: &Config, settings: &NetworkSettings) -> Result<()> {
    tokio::fs::create_dir_all(Path::new(cfg.config_dir_path())).await?;

    let mut file = File::create(Path::new(
        format!(settings_file_path!(), cfg.config_dir_path(), cfg.name()).as_str(),
    ))
    .await?;
    let data = serde_json::to_string_pretty(settings).map_err(|e| Error::Io(e.into()))?;
    file.write_all(data.as_bytes()).await?;

    Ok(())
}

#[instrument(name = "", skip(cfg), fields(name = %cfg.name()))]
pub async fn run(cfg: Config) -> Result<ClusterHandle> {
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    // configure server
    let (server_cfg, _cert) = crate::config::server::self_signed(cfg.quic())?;

    // configure client
    let client = Arc::new(Client::new(cfg.quic(), cfg.client())?);

    // discover who I am
    let settings = match load_settings(&cfg).await {
        Ok(settings) => settings,
        Err(_) => {
            let settings = NetworkSettings {
                node_id: Id::default(),
            };
            persist_settings(&cfg, &settings)
                .await
                .map_err(|e| Error::Bootstrap(format!("unable to save settings; source: {}", e)))?;
            settings
        }
    };

    let address = Address(format!("127.0.0.1:{}", cfg.server().port));
    let node = Node::new(settings.node_id, address);
    let membership = Membership::new(node, cfg.gossip());

    // discover nat capabilities

    // gossip _task_
    let gossip = crate::gossip::run(cfg.gossip(), Arc::clone(&client), Arc::clone(&membership));

    // configure context
    let shared = Arc::new(Shared {
        membership: Arc::clone(&membership),
        config: cfg,
        gossip,
        client: Arc::clone(&client),
    });

    // swim _task_
    let _ = crate::membership::swim::run(shared.config.swim(), shared.clone());

    // return api which is exposing send bi/uni/udp messages
    let cluster = Cluster {
        shared: Arc::clone(&shared),
        server_cfg,
        shutdown_tx,
    };

    // setup server _task_
    let ctx = Arc::clone(&cluster.shared);
    crate::server::run(ctx, cluster.server_cfg.clone()).await?;

    // join
    cluster.join().await?;

    Ok(ClusterHandle { cluster })
}

impl Cluster {
    async fn leave(self, gracefully: bool) -> Result<()> {
        // TODO
        if !gracefully {
            drop(self.shutdown_tx);
            return Ok(());
        }

        self.shared
            .gossip()
            .queue(Multicast::with_payload(Message::Leave(Peer::from(
                self.shared.membership().me(),
            ))));

        // random sleep, maybe later on provide a gossip response
        tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;
        Ok(())
    }

    async fn join(&self) -> crate::Result<()> {
        let bootstrap_nodes = self.shared.config.network().bootstrap_nodes();
        let mut connection = match self.connect_any(bootstrap_nodes).await? {
            None => return Ok(()),
            Some(connection) => connection,
        };

        // Send _join_ message and update members
        let (_, mut recv_stream) = connection
            .send_bi(&Message::Join(Peer::from(self.shared.membership.me())))
            .await
            .map_err(|e| {
                Error::Bootstrap(format!("bootstrap node is not responding; source: {}", e))
            })?;

        let response = Message::read(&mut recv_stream)
            .await
            .map_err(|e| Error::Bootstrap(format!("unable to read join response; source: {}", e)))?
            .unwrap();

        match response {
            Message::Membership(peers) => {
                peers.into_iter().for_each(|peer| {
                    self.shared.membership.insert(peer);
                });
                Ok(())
            }
            _ => Err(Error::Bootstrap(
                "bootstrap node responded with wrong message type".into(),
            )),
        }
    }

    async fn connect_any(
        &self,
        bootstrap_nodes: &[Address],
    ) -> Result<Option<ConnectionHolder>, crate::Error> {
        if bootstrap_nodes.is_empty() {
            warn!("no bootstrap nodes defined; starting on my own");
            return Ok(None);
        }

        // Attempt to connect to all nodes and return the first one to succeed
        let mut bootstrap_nodes = bootstrap_nodes
            .iter()
            .filter(|&addr| addr != self.shared.membership.me().address())
            .peekable();

        if bootstrap_nodes.peek().is_none() {
            warn!("I'm bootstrap node; starting on my own");
            return Ok(None);
        }

        let bootstrap_nodes: Vec<BoxFuture<_>> = bootstrap_nodes
            .map(|addr| async move { self.shared.client.connect_to(&addr).await }.boxed())
            .collect();

        match futures::future::select_ok(bootstrap_nodes).await {
            Err(e) => {
                warn!(
                    "starting on my own; unable to connect to any bootstrap node: {reason}",
                    reason = e
                );
                Ok(None)
            }
            Ok(conn) => Ok(Some(conn.0)),
        }
    }
}

pub struct ClusterHandle {
    cluster: Cluster,
}

impl ClusterHandle {
    pub async fn leave(self, gracefully: bool) -> Result<()> {
        self.cluster.leave(gracefully).await
    }

    pub fn peers(&self) -> HashMap<PeerId, PeerInner> {
        self.cluster.shared.membership.peers()
    }

    pub fn peers_len(&self) -> usize {
        self.cluster.shared.membership.len()
    }

    pub fn node(&self) -> &Node {
        self.cluster.shared.membership.me()
    }
}
