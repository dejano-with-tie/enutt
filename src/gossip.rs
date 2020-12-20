use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use futures::future::FutureExt;
use futures::{StreamExt, TryFutureExt};
use parking_lot::{Mutex, RwLock};
use rand::rngs::ThreadRng;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, info_span, instrument, warn};
use tracing_futures::Instrument;

use crate::connection::Connection;
use crate::message::{Message, Multicast, MulticastId, MulticastInner};
use crate::node::{Node, Peer, PeerId, PeerInner};

struct MulticastWrapper {
    pub hop_count: usize,
    pub message: Multicast,
}

impl Display for MulticastWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(c:{}) {}", self.hop_count, self.message)
    }
}

impl MulticastWrapper {
    pub fn new(message: Multicast) -> Self {
        MulticastWrapper {
            hop_count: Default::default(),
            message,
        }
    }
    pub fn hop_count(&self) -> usize {
        self.hop_count
    }
    pub fn inc_hop_count(&mut self) {
        self.hop_count += 1;
    }
}

/// Start Gossip task in background
///
/// Return sender which is used to queue new multicasts
#[instrument(skip(sender), name = "gossip")]
pub fn run(sender: Arc<Node>) -> UnboundedSender<Multicast> {
    // TODO: protocol_period // after what time to stop
    let cycle_count = 4;
    let gossip = Arc::new(Gossip {
        sender,
        queue: RwLock::new(vec![]),
        seen: RwLock::new(vec![]),
        fanout: 4,            // TODO: Config
        cycle_frequency: 300, // TODO: Config
        cycle_rate: 4,        // TODO: Config
        cycle_count,
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

/// queue multicasts
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

    fanout: usize,
    cycle_frequency: u64,
    /// current node
    sender: Arc<Node>,
    cycle_rate: usize,
    // should be calculated at runtime; GOSSIP_RATE * (((member_count + 1) as f64).ln().ceil() as usize);
    // where gossip rate is arbitrary number [1..6], maybe >=2 && 4 <=
    cycle_count: usize,
}
/// Implements simple gossip based protocol
impl Gossip {
    pub async fn run(self: &Arc<Self>) -> crate::Result<()> {
        let mut round_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.cycle_frequency));
        loop {
            let now = round_interval.tick().await;
            debug!("starting round (Time now = {:?})", now);

            // TODO: Make sure all multicasts in queue are unique (could receive two same multicasts and queued them both)
            self.cycle();
        }
    }

    fn cycle(self: &Arc<Self>) {
        let mut queue_w = self.queue.write();
        // remove messages which have been sent > _cycle_count_ times
        queue_w.retain(|multicast| multicast.hop_count() < self.cycle_count);
        // inc hop count
        queue_w
            .iter_mut()
            .for_each(|multicast| multicast.inc_hop_count());

        drop(queue_w);

        // clone peers here and propagate them into calling fn down (use reference!)

        // multicast
        let queue_r = self.queue.read();
        let infected: HashMap<MulticastId, Option<Arc<Vec<Peer>>>> = queue_r
            .iter()
            .map(|multicast| {
                info!("execute {}", multicast);
                match self.multicast(&multicast.message) {
                    None => (*multicast.message.id(), None),
                    Some(peers) => (*multicast.message.id(), Some(peers)),
                }
            })
            .collect();
        drop(queue_r);

        // TODO: Write infected before sending!
        // write infected
        let mut queue_w = self.queue.write();
        queue_w.iter_mut().for_each(|multicast| {
            if let Some(Some(peers)) = infected.get(multicast.message.id()) {
                peers.iter().for_each(|peer| {
                    multicast.message.insert_infected(*peer.id());
                });
            }
        });
        drop(queue_w);
    }

    pub fn is_seen(&self, id: &MulticastId) -> bool {
        self.seen.read().contains(id)
    }

    pub fn mark_as_seen(&self, id: MulticastId) {
        self.seen.write().push(id);
    }

    pub fn multicast(&self, multicast: &Multicast) -> Option<Arc<Vec<Peer>>> {
        // update multicast state
        // multicast.infected.insert(self.sender.id());
        let peers = match self.random_peers(multicast.infected()) {
            Some(peers) => Arc::new(peers),
            None => return None,
        };

        let multicast = multicast.clone();
        // send actual message
        let peers_to_msg = Arc::clone(&peers);
        tokio::spawn(async move {
            let messages =
                futures::stream::iter(peers_to_msg.iter()).for_each_concurrent(None, |peer| {
                    let multicast = multicast.clone();
                    async move {
                        Connection::send_uni(
                            peer.inner().address(),
                            &Message::Multicast(multicast),
                        )
                        .await
                        .unwrap();
                    }
                });

            messages.await
        });

        Some(peers)
    }

    // /// TODO: Maybe pass in message origin
    fn random_peers(&self, infected: &HashSet<PeerId>) -> Option<Vec<Peer>> {
        let peers = &self.sender.peers().read();

        // TODO: What if message is sent to same peer
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
