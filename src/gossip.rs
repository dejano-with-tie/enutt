use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use futures::StreamExt;
use parking_lot::{RwLock, RwLockWriteGuard};
use tracing::{debug, info, info_span, instrument, warn};
use tracing_futures::Instrument;

use crate::client::Client;
use crate::membership::{Membership, Peer};
use crate::message::{Message, Multicast, MulticastId};
use crate::node::NodeId;
use crate::SliceDisplay;

/// Wrap `Multicast` message with `hop_count`
struct DisseminationMessage {
    /// how many times this message has been gossiped
    pub period_cnt: usize,
    pub message: Multicast,
}

impl Display for DisseminationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(c:{}) {}", self.period_cnt, self.message)
    }
}

impl DisseminationMessage {
    pub fn new(message: Multicast) -> Self {
        DisseminationMessage {
            period_cnt: Default::default(),
            message,
        }
    }
    pub fn period_cnt(&self) -> usize {
        self.period_cnt
    }
    pub fn inc_period_cnt(&mut self) {
        self.period_cnt += 1;
    }
}

pub struct DisseminationQueue {
    queue: Arc<RwLock<Vec<DisseminationMessage>>>,
    /// Already gossiped about these. Used for filtering when queueing new multicasts
    seen: RwLock<Vec<MulticastId>>,
    sender_id: NodeId,
}

impl DisseminationQueue {
    /// Queue messages
    /// Every multicast has to go through this fn before it is queued
    #[instrument(name = "dissemination", skip(self, multicast), fields(message = %multicast))]
    pub fn queue(&self, mut multicast: Multicast) {
        if self.is_seen(multicast.id()) {
            debug!("skip");
            return;
        }
        // micro optimizations:
        // 1. if we got `swim::failed` msg to queue -> remove all other state msgs related to failed peer from queue
        // because failed overrides all other states

        debug!("queue");
        self.mark_as_seen(*multicast.id());

        // we are infected now
        multicast.insert_infected(self.sender_id);

        self.queue
            .write()
            .push(DisseminationMessage::new(multicast));
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
#[instrument(skip(cfg, client, membership), name = "gossip")]
pub fn run(
    cfg: &crate::config::Gossip,
    client: Arc<Client>,
    membership: Arc<Membership>,
) -> DisseminationQueue {
    let sender_id = *membership.me().id();

    let gossip = Gossip {
        membership,
        client,
        // TODO: Do a priority queue here! Least gossiped messages should be a priority
        queue: Arc::new(RwLock::new(vec![])),
        fanout: cfg.fanout(),
        period_frequency: cfg.frequency(),
        period_rate: cfg.period_rate(),
    };

    let gossip_queue = DisseminationQueue {
        seen: RwLock::new(vec![]),
        queue: gossip.queue.clone(),
        sender_id,
    };

    // run gossip protocol
    tokio::spawn(async move { gossip.run().await }.instrument(info_span!("period")));

    gossip_queue
}

pub struct Gossip {
    membership: Arc<Membership>,
    /// To gossip about in next period
    queue: Arc<RwLock<Vec<DisseminationMessage>>>,
    /// Quic client
    client: Arc<Client>,
    fanout: usize,
    period_frequency: u64,
    period_rate: usize,
}

/// Each message is multicasted at most periods_to_spread times
pub fn periods_to_spread(period_rate: usize, members_len: usize) -> usize {
    period_rate * ((members_len as f64).ln_1p().ceil() as usize)
}

/// Implements simple gossip based protocol
impl Gossip {
    pub async fn run(&self) -> crate::Result<()> {
        let mut period_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.period_frequency));
        loop {
            let now = period_interval.tick().await;
            debug!("starting period (Time now = {:?})", now);

            let members_len = self.membership.len();
            let periods_to_spread = periods_to_spread(self.period_rate, members_len);
            debug!(
                "membership size: {}; period rate: {}",
                members_len, periods_to_spread
            );

            // TODO: Make sure all multicasts in queue are unique (could receive two same multicasts and queued them both)
            self.period(periods_to_spread);
        }
    }

    /// Gossip period which is executed every `period_frequency` milliseconds.
    ///
    /// In one period, each message from `queue` is gossiped to `fanout` randomly selected peers.
    /// If message is gossiped `periods_to_spread` times, it will be removed from `queue`.
    ///
    /// To improve selection of random peers, `infected` list is passed on with message so next
    /// peer won't gossip to already infected peers.
    fn period(&self, periods_to_spread: usize) {
        let mut messages = self.queue.write();
        // remove messages which have been sent > _periods_to_spread_ times
        messages.retain(|multicast| multicast.period_cnt() < periods_to_spread);

        // inc hop count and update infected
        messages.iter_mut().for_each(|multicast| {
            // we increase hop count even if there are no nodes to which we could gossip to
            // Another way to do it would be to increase only when it is sent to someone else
            // but do also time based deletion (message can expire + hop_count_limit)
            multicast.inc_period_cnt();
        });

        let messages = RwLockWriteGuard::downgrade(messages);
        messages.iter().for_each(|multicast| {
            if let Some(peers) = self
                .membership
                .random_samples(self.fanout, multicast.message.infected())
            {
                debug!("execute {}; targets {}", multicast, SliceDisplay(&peers));
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
                    if let Err(e) = client
                        .send_uni(peer.inner().address(), &Message::Disseminate(multicast))
                        .await
                    {
                        warn!("failed to multicast; {}", e);
                    }
                }
            });

            messages.await
        });
    }
}
