use std::cmp::min;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use futures::StreamExt;
use parking_lot::{
    MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard,
};
use rand::rngs::ThreadRng;
use rand::Rng;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, info, info_span, instrument, warn};
use tracing_futures::Instrument;

use crate::client::Client;
use crate::membership::Membership;
use crate::message::{Message, Multicast, MulticastId};
use crate::node::{Node, NodeId, Peer, PeerId, PeerInner};
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

pub struct GossipQueue {
    queue: Arc<RwLock<Vec<MulticastWrapper>>>,
    /// Already gossiped about these. Used for filtering when queueing new multicasts
    seen: RwLock<Vec<MulticastId>>,
    sender_id: NodeId,
}

impl GossipQueue {
    /// Queue messages
    /// Every multicast has to go through this fn before it is queued
    pub fn queue(&self, mut multicast: Multicast) {
        if self.is_seen(multicast.id()) {
            warn!("skip; {}", multicast);
            return;
        }

        info!("queue {}", multicast);
        self.mark_as_seen(*multicast.id());

        // we are infected now
        multicast.insert_infected(self.sender_id);

        self.queue.write().push(MulticastWrapper::new(multicast));
    }

    pub fn peek(&mut self) {}

    fn is_seen(&self, id: &MulticastId) -> bool {
        self.seen.read().contains(id)
    }

    fn mark_as_seen(&self, id: MulticastId) {
        self.seen.write().push(id);
    }
}

/// Start Gossip task in background
///
/// Return sender which is used to queue new multicasts
#[instrument(skip(sender, cfg, client, membership), name = "gossip")]
pub fn run(
    cfg: &crate::config::Gossip,
    client: Arc<Client>,
    sender: Arc<Node>,
    membership: Arc<Membership>,
) -> GossipQueue {
    let gossip = Arc::new(Gossip {
        membership,
        client,
        // TODO: move this to separate struct
        // TODO: Do a priority queue here! REALLY DO THIS ONE
        queue: Arc::new(RwLock::new(vec![])),
        fanout: cfg.fanout(),
        cycle_frequency: cfg.frequency(),
        cycle_rate: cfg.rate(),
    });

    let gossip_queue = GossipQueue {
        seen: RwLock::new(vec![]),
        queue: Arc::clone(&gossip.queue),
        sender_id: sender.id(),
    };

    // run gossip protocol
    tokio::spawn(async move { gossip.run().await }.instrument(info_span!("cycle")));

    gossip_queue
}

pub struct Gossip {
    membership: Arc<Membership>,
    /// To gossip about in next cycle
    queue: Arc<RwLock<Vec<MulticastWrapper>>>,
    /// Quic client
    client: Arc<Client>,
    fanout: usize,
    cycle_frequency: u64,
    cycle_rate: usize,
}
/// Implements simple gossip based protocol
impl Gossip {
    pub async fn run(self: &Arc<Self>) -> crate::Result<()> {
        let mut cycle_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.cycle_frequency));
        loop {
            let now = cycle_interval.tick().await;
            debug!("starting cycle (Time now = {:?})", now);

            let members_len = self.membership.len();
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
        let mut messages = self.queue.write();
        // remove messages which have been sent > _cycle_count_ times
        messages.retain(|multicast| multicast.cycle_cnt() < cycle_rate);

        // inc hop count and update infected
        messages.iter_mut().for_each(|multicast| {
            // we increase hop count even if there are no nodes to which we could gossip to
            // Another way to do it would be to increase only when it is sent to someone else
            // but do also time based deletion (message can expire + hop_count_limit)
            multicast.inc_cycle_cnt();
        });

        // NOTE: Micro optimization: When queue is too big, do a piggyback of messages. The question is, what is too large?
        // nm_messages = queue_size * fanout;
        // - queue_size * fanout >=
        // gossip
        let messages = RwLockWriteGuard::downgrade(messages);
        messages.iter().for_each(|multicast| {
            if let Some(peers) = self
                .membership
                .random(self.fanout, multicast.message.infected())
            {
                info!("execute {}; targets {}", multicast, SliceDisplay(&peers));
                self.multicast(&multicast.message, &Arc::new(peers))
            };
        });
    }

    pub fn multicast(&self, multicast: &Multicast, peers: &Arc<Vec<Peer>>) {
        let multicast = multicast.clone();
        // send actual message
        let peers = Arc::clone(peers);
        let client = Arc::clone(&self.client);
        tokio::spawn(async move {
            let messages = futures::stream::iter(peers.iter()).for_each_concurrent(None, |peer| {
                // NOTE: This cloning is madness
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
}
