use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use futures::StreamExt;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rand::rngs::ThreadRng;
use rand::Rng;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, info, info_span, instrument, warn};
use tracing_futures::Instrument;

use crate::client::Client;
use crate::connection::ConnectionHolder;
use crate::message::{Message, Multicast, MulticastId};
use crate::node::{Node, Peer, PeerId, PeerInner};
use crate::SliceDisplay;

/// Wrap `Multicast` message with `hop_count`
struct MulticastWrapper {
    /// how many times this message has been gossiped
    pub cycle_cnt: usize,
    pub message: Multicast,
}

impl Display for MulticastWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(c:{}) {}", self.cycle_cnt, self.message)
    }
}

impl MulticastWrapper {
    pub fn new(message: Multicast) -> Self {
        MulticastWrapper {
            cycle_cnt: Default::default(),
            message,
        }
    }
    pub fn cycle_cnt(&self) -> usize {
        self.cycle_cnt
    }
    pub fn inc_cycle_cnt(&mut self) {
        self.cycle_cnt += 1;
    }
}

/// Start Gossip task in background
///
/// Return sender which is used to queue new multicasts
#[instrument(skip(sender, cfg, client), name = "gossip")]
pub fn run(
    cfg: &crate::config::Gossip,
    client: Arc<Client>,
    sender: Arc<Node>,
) -> UnboundedSender<Multicast> {
    let gossip = Arc::new(Gossip {
        sender,
        client,
        // TODO: move this to separate struct
        queue: RwLock::new(vec![]),
        seen: RwLock::new(vec![]),
        fanout: cfg.fanout(),
        cycle_frequency: cfg.frequency(),
        cycle_rate: cfg.rate(),
    });

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    // queue messages
    tokio::spawn(queue_multicast(rx, Arc::clone(&gossip)).in_current_span());

    // run gossip protocol
    let run_gossip = Arc::clone(&gossip);
    let gossip_round_task = async move { run_gossip.run().await };
    tokio::spawn(gossip_round_task.instrument(info_span!("round")));

    tx
}

/// Queue messages
/// Every multicast has to go through this fn before it is queued
async fn queue_multicast(mut rx: UnboundedReceiver<Multicast>, gossip: Arc<Gossip>) {
    while let Some(mut multicast) = rx.recv().await {
        // skip if multicast has been seen already
        if gossip.is_seen(multicast.id()) {
            warn!("skip; {}", multicast);
            continue;
        }

        info!("queue {}", multicast);
        gossip.mark_as_seen(*multicast.id());

        // we are infected now
        multicast.insert_infected(gossip.sender.id());

        gossip.queue.write().push(MulticastWrapper::new(multicast));
    }
}

pub struct Gossip {
    /// To gossip about in next round
    queue: RwLock<Vec<MulticastWrapper>>,
    /// Already gossiped about these. Used for filtering when queueing new multicasts
    seen: RwLock<Vec<MulticastId>>,
    /// Quic client
    client: Arc<Client>,
    fanout: usize,
    cycle_frequency: u64,
    /// current node
    sender: Arc<Node>,
    cycle_rate: usize,
}
/// Implements simple gossip based protocol
impl Gossip {
    pub async fn run(self: &Arc<Self>) -> crate::Result<()> {
        let mut round_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.cycle_frequency));
        loop {
            let now = round_interval.tick().await;
            debug!("starting round (Time now = {:?})", now);

            let members_len = { self.sender.peers().read().len() };
            let cycle_rate = self.cycle_rate * (((members_len + 1) as f64).ln().ceil() as usize);
            debug!(
                "membership size: {}; cycle rate: {}",
                members_len, cycle_rate
            );

            // TODO: Make sure all multicasts in queue are unique (could receive two same multicasts and queued them both)
            self.cycle(cycle_rate);
        }
    }

    /// Gossip cycle which is executed every `cycle_frequency` milliseconds.
    ///
    /// In one cycle, each message from `queue` is gossiped to `fanout` randomly selected peers.
    /// If message is gossiped `cycle_rate` times, it will be removed from `queue`.
    ///
    /// To improve selection of random peers, `infected` list is passed on with message so next
    /// peer won't gossip to already infected peers.
    fn cycle(self: &Arc<Self>, cycle_rate: usize) {
        let mut lock = self.queue.write();
        // remove messages which have been sent > _cycle_count_ times
        lock.retain(|multicast| multicast.cycle_cnt() < cycle_rate);

        let lock = RwLockWriteGuard::downgrade_to_upgradable(lock);
        // select random peers to gossip to
        let rnd_peers: HashMap<MulticastId, Option<Arc<Vec<Peer>>>> = lock
            .iter()
            .map(|multicast| {
                let peers = match self.random_peers(multicast.message.infected()) {
                    Some(peers) => Arc::new(peers),
                    None => return (*multicast.message.id(), None),
                };
                (*multicast.message.id(), Some(peers))
            })
            .collect();

        let mut lock = RwLockUpgradableReadGuard::upgrade(lock);
        // inc hop count and update infected
        lock.iter_mut().for_each(|multicast| {
            // we increase hop count even if there are no nodes to which we could gossip to
            // Another way to do it would be to increase only when it is sent to someone else
            // but do also time based deletion (message can expire + hop_count_limit)
            multicast.inc_cycle_cnt();

            if let Some(peers) = rnd_peers.get(multicast.message.id()).unwrap() {
                peers.iter().for_each(|peer| {
                    multicast.message.insert_infected(*peer.id());
                });
            }
        });

        // gossip
        let lock = RwLockWriteGuard::downgrade(lock);
        lock.iter().for_each(|multicast| {
            if let Some(peers) = rnd_peers.get(multicast.message.id()).unwrap() {
                info!(
                    "execute {}; targets {}",
                    multicast,
                    SliceDisplay(peers, &true)
                );
                self.multicast(&multicast.message, peers);
            }
        });
    }

    pub fn is_seen(&self, id: &MulticastId) -> bool {
        self.seen.read().contains(id)
    }

    pub fn mark_as_seen(&self, id: MulticastId) {
        self.seen.write().push(id);
    }

    pub fn multicast(&self, multicast: &Multicast, peers: &Arc<Vec<Peer>>) {
        let multicast = multicast.clone();
        // send actual message
        let peers_to_msg = Arc::clone(peers);
        let client = Arc::clone(&self.client);
        tokio::spawn(async move {
            let messages =
                futures::stream::iter(peers_to_msg.iter()).for_each_concurrent(None, |peer| {
                    // TODO: This cloning is madness
                    let multicast = multicast.clone();
                    let client = Arc::clone(&client);
                    async move {
                        client
                            .send_uni(peer.inner().address(), &Message::Multicast(multicast))
                            .await
                            .unwrap();
                    }
                });

            messages.await
        });
    }

    fn random_peers(&self, infected: &HashSet<PeerId>) -> Option<Vec<Peer>> {
        let peers = &self.sender.peers().read();

        let peers: Vec<(&PeerId, &PeerInner)> = peers
            .iter()
            // skip myself
            .filter(|(id, _)| *id != &self.sender.id())
            // skip infected peers
            .filter(|(id, _)| !infected.iter().any(|peer| *id == peer))
            .map(|(id, peer)| (id, peer))
            .collect();

        let peers_len = peers.len();

        match peers_len {
            0 => None,
            1 => Some(vec![Peer::from(*peers.get(0).unwrap())]),
            _ => {
                let fanout = min(self.fanout, peers_len);
                Some(
                    (0..fanout)
                        .map(|_i| {
                            Peer::from(
                                *peers
                                    .get(ThreadRng::default().gen_range(0, peers_len))
                                    .unwrap(),
                            )
                        })
                        .collect(),
                )
            }
        }
    }
}
