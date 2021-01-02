use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Sender;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{error, info, info_span};
use tracing_futures::Instrument;
use uuid::Uuid;

use crate::config::Gossip;
use crate::node::{Address, Node};
use crate::{Id, IdGen};

pub mod swim;

/// Type alias for convenience
pub type SuspectedKey = (Instant, u64);

pub struct State {
    /// Map holding all known peers: healthy, suspected and myself (_Node_)
    peers: HashMap<PeerId, PeerInner>,

    /// This map is used to track when peer should transition from `suspected` to `failed` state.
    /// Key is `instant` at which point peer will transition `failed` state.
    /// As we don't keep track of failed peers, transition simply means removing
    /// entry from this map (expire).
    ///
    /// `BTreeMap` is used to maintain order.
    ///
    /// It is very unlikely for two peers to fail at same `instant` but just to be sure we provide
    /// a key with unique `u64`.
    suspected: BTreeMap<SuspectedKey, PeerId>,

    /// Used to generate next unique identifier for `suspected` map.
    id_gen: IdGen,
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.suspected.keys().next().map(|expiration| expiration.0)
    }
}

pub struct Membership {
    #[allow(unused)]
    me: Node,

    /// Membership state guarded by a `RwLock`.
    state: RwLock<State>,

    /// Used as a _waker_ to _wake_ background task which is responsible for
    /// keeping `state#suspected` collection clean
    purge_task: Arc<Notify>,

    /// Broadcast transition event from `suspected` to `failed`
    failed: Sender<Peer>,
    gossip_rate: usize,
    gossip_frequency: u64,
}

impl Membership {
    /// Create new instance of membership with state containing myself (_Node_).
    /// Spawns background task responsible for moving peers from `suspected` to `failed` state
    pub fn new(node: Node, gossip_cfg: &Gossip) -> Arc<Self> {
        let mut peers = HashMap::new();
        let myself = Peer::from(&node);
        peers.insert(*myself.id(), myself.inner().clone());

        let state = RwLock::new(State {
            peers,
            suspected: BTreeMap::new(),
            id_gen: IdGen::default(),
        });

        let (failed, _) = tokio::sync::broadcast::channel(128);

        let instance = Arc::new(Self {
            me: node,
            state,
            purge_task: Arc::new(Notify::new()),
            failed,
            gossip_rate: gossip_cfg.period_rate(),
            gossip_frequency: gossip_cfg.frequency(),
        });

        // start background purge task
        let purge_instance = instance.clone();
        tokio::spawn(
            async move {
                purge(purge_instance).await;
            }
            .instrument(info_span!("purge")),
        );

        instance
    }

    /// Inserts new peer into membership list.
    ///
    /// If provided peer is already known, it will be moved
    /// to `alive` state and return value will be `None`
    ///
    /// If provided peer is new peer, return value will be `Some(Peer)`
    pub fn insert(&self, peer: Peer) -> Option<Peer> {
        let (id, inner) = peer.into();

        let mut state = self.state.write();

        let old_peer = state.peers.insert(id, inner.clone());

        // If it is known peer and it was in `suspected` state, move it to alive state
        if let Some(old_peer) = old_peer {
            if let PeerState::Suspected(key) = old_peer.state {
                state.suspected.remove(&key);
            }
            return None;
        }

        let peer = Peer::from((id, inner));
        info!("new peer: {}", peer);
        Some(peer)
    }

    /// Remove all traces for given peer
    ///
    pub fn remove(&self, peer_id: &PeerId) {
        let mut state = self.state.write();
        if let Some(peer) = state.peers.remove(peer_id) {
            if let PeerState::Suspected(key) = peer.state {
                state.suspected.remove(&key);
            }
        }
    }

    /// Update peer's state to `suspected`.
    ///
    /// State update is based on `peer#incarnation` number with following rules:
    ///
    /// Given peer has `incarnation` number `i`
    ///
    /// * If existing peer's current state is `alive` with incarnation number `j`
    ///     - when i >= j -> _transition_ to `suspected` state
    ///     - otherwise do nothing
    /// * If existing peer's current state is `suspected` with incarnation number `j`
    ///     - when i > j -> _update_ suspected expiration time
    ///     - otherwise do nothing
    ///
    /// If update is performed, return value will be `Some(peer)`
    /// If no action is performed, return value will be `None`
    pub fn suspect(&self, peer: Peer) -> Option<Peer> {
        let i = peer.inner().incarnation;
        let (j, current_state) = {
            let state = self.state.read();
            match state.peers.get(peer.id()) {
                Some(peer) => (peer.incarnation, peer.state.clone()),
                None => return None,
            }
        };

        let ttl = self.ttl();
        let expire_at = Instant::now() + ttl;

        let state = self.state.upgradable_read();

        let should_notify = state
            .next_expiration()
            .map(|expiration| expiration > expire_at)
            .unwrap_or(true);

        let mut state = RwLockUpgradableReadGuard::upgrade(state);
        let mut updated = false;
        match current_state {
            PeerState::Alive if i >= j => {
                // move to `suspected` state
                let key = (expire_at, state.id_gen.next());
                let existing = state.peers.get_mut(peer.id()).unwrap();
                existing.state = PeerState::Suspected(key);
                state.suspected.insert(key, *peer.id());
                updated = true;
            }
            PeerState::Suspected(key) if i > j => {
                // update expiration time, keep `suspected` state
                state.suspected.remove(&key);
                state.suspected.insert((expire_at, key.1), *peer.id());
                updated = true;
            }
            _ => {}
        };

        let state = RwLockWriteGuard::downgrade(state);

        if updated && should_notify {
            drop(state);
            self.purge_task.notify();
        }

        match updated {
            true => Some(peer),
            false => None,
        }
    }

    /// Update peer's state to `alive`.
    ///
    /// State update is based on `peer#incarnation` number with following rules:
    ///
    /// Given peer has `incarnation` number `i`
    ///
    /// * If existing peer's current state is `alive` with incarnation number `j`
    ///     - when i > j -> confirm `alive` state
    ///     - otherwise do nothing
    /// * If existing peer's current state is `suspected` with incarnation number `j`
    ///     - when i > j -> transition to `alive` state
    ///     - otherwise do nothing
    ///
    /// If peer is not present in membership list, it will be inserted with `alive` state.
    ///
    /// If update is performed, return value will be `Some(peer)`
    /// If no action is performed, return value will be `None`
    pub fn alive(&self, peer: Peer) -> Option<Peer> {
        let mut state = self.state.write();
        let existing = match state.peers.get_mut(peer.id()) {
            Some(peer) => peer,
            None => return None,
        };

        let i = peer.inner().incarnation;
        let j = existing.incarnation;

        match existing.state {
            PeerState::Alive if i > j => {
                // confirm alive?
                return Some(peer);
            }
            PeerState::Suspected(key) if i > j => {
                // change state to alive
                existing.state = PeerState::Alive;
                state.suspected.remove(&key);
                return Some(peer);
            }
            _ => {}
        };

        None
    }

    pub fn is_suspected(&self, id: &PeerId) -> bool {
        let state = self.state.read();
        if let Some(peer) = state.peers.get(id) {
            if let PeerState::Alive = peer.state {
                return true;
            }
        }

        false
    }

    /// Time-to-live node in `suspected` state before moving to `failed` state.
    ///
    /// ttl is calculated as gossip's estimate of number of cycles needed for message to reach
    /// every peer * gossip frequency (ms)
    fn ttl(&self) -> Duration {
        let periods = crate::gossip::periods_to_spread(self.gossip_rate, self.len());
        Duration::from_millis((periods as u64) * self.gossip_frequency)
    }

    pub fn len(&self) -> usize {
        self.state.read().peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn peers_clone(&self) -> Vec<Peer> {
        self.state
            .read()
            .peers
            .iter()
            .map(|(id, p)| Peer::from((id, p)))
            .collect()
    }

    pub fn shuffled(&self) -> Vec<Peer> {
        let mut shuffled: Vec<Peer> = self
            .peers_clone()
            .into_iter()
            .filter(|peer| peer.id() != self.me.id())
            .collect();
        shuffled.shuffle(&mut ThreadRng::default());
        shuffled
    }

    pub fn random_samples(&self, samples: usize, to_ignore: &HashSet<PeerId>) -> Option<Vec<Peer>> {
        let state = self.state.read();

        let peers: Vec<(&PeerId, &PeerInner)> = state
            .peers
            .iter()
            // skip myself
            .filter(|(id, _)| *id != self.me.id())
            // skip infected peers
            .filter(|(id, _)| !to_ignore.iter().any(|peer| *id == peer))
            .map(|(id, peer)| (id, peer))
            .collect();

        let peers_len = peers.len();

        match peers_len {
            0 => None,
            1 => Some(vec![Peer::from(*peers.get(0).unwrap())]),
            _ => {
                let rand_idx = Membership::rand_indexes(samples, peers_len);
                Some(
                    rand_idx
                        .iter()
                        .map(|i| Peer::from(*peers.get(*i).unwrap()))
                        .collect(),
                )
            }
        }
    }

    fn rand_indexes(samples: usize, high: usize) -> HashSet<usize> {
        let samples = min(samples, high);
        let mut rand_idx: HashSet<usize> = HashSet::with_capacity(samples);
        loop {
            match rand_idx.len() {
                len if { len < samples } => {
                    rand_idx.insert(ThreadRng::default().gen_range(0, high));
                }
                _ => break,
            }
        }
        rand_idx
    }
    pub fn me(&self) -> &Node {
        &self.me
    }

    // TODO: Only for test env
    pub fn peers(&self) -> HashMap<PeerId, PeerInner> {
        self.state.read().peers.clone()
    }
}

async fn purge(membership: Arc<Membership>) {
    loop {
        if let Some(when) = purge_expired_keys(&membership) {
            tokio::select! {
                _ = tokio::time::delay_until(when) => {}
                _ = membership.purge_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            membership.purge_task.notified().await;
        }
    }
}

fn purge_expired_keys(membership: &Arc<Membership>) -> Option<Instant> {
    let mut state = membership.state.write();

    // satisfy borrow checker by providing reference behind RwLockWriteGuard
    let state = &mut *state;

    // Find all keys scheduled to expire **before** now.
    let now = Instant::now();

    while let Some((&(when, id), peer_id)) = state.suspected.iter().next() {
        if when > now {
            // Done purging, `when` is the instant at which the next key
            // expires. The worker task will wait until this instant.
            return Some(when);
        }

        if let Some(inner) = state.peers.get(peer_id) {
            let peer = Peer::from((peer_id, inner));
            if let Err(e) = membership.failed.send(peer) {
                error!("failed to broadcast about failed peer: {:?}", e);
            }
        }

        error!("transition peer to FAILED state {:?}", id);
        state.peers.remove(peer_id);
        state.suspected.remove(&(when, id));
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::node::Address;

    use super::*;

    #[test]
    pub fn random_samples() {
        let mut peers = HashMap::with_capacity(10);
        for i in 0..10 {
            peers.insert(PeerId::default(), PeerInner::new(Address(i.to_string()), 0));
        }

        // TODO
        // let membership = Membership {
        //     peers: RwLock::new(peers),
        // };
        //
        // let samples = membership.random_samples(4, &HashSet::new());
        // assert!(samples.is_some());
        //
        // let samples = samples.unwrap();
        //
        // assert_eq!(4, samples.len());
    }
}

/// Represents arbitrary _node_ in a cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Peer(PeerId, PeerInner);

impl Peer {
    pub fn new(address: Address, uuid: PeerId, incarnation: u64) -> Self {
        Peer(uuid, PeerInner::new(address, incarnation))
    }

    pub fn id(&self) -> &PeerId {
        &self.0
    }

    pub fn inner(&self) -> &PeerInner {
        &self.1
    }

    pub fn into_inner(self) -> PeerInner {
        self.1
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Peer({}, {}, {})", self.0, self.1, self.1.state)
    }
}

impl From<&Node> for Peer {
    fn from(node: &Node) -> Self {
        Peer::new(node.address().clone(), *node.id(), node.incarnation())
    }
}

impl From<(&PeerId, &PeerInner)> for Peer {
    fn from(entry: (&PeerId, &PeerInner)) -> Self {
        Peer(*entry.0, entry.1.clone())
    }
}

impl From<(PeerId, PeerInner)> for Peer {
    fn from(t: (PeerId, PeerInner)) -> Self {
        Peer(t.0, t.1)
    }
}

#[derive(Clone, Debug)]
pub enum PeerState {
    Alive,
    Suspected(SuspectedKey),
}

impl Display for PeerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerState::Alive => write!(f, "Alive"),
            PeerState::Suspected(_) => write!(f, "Suspected"),
        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        PeerState::Alive
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInner {
    address: Address,
    pub incarnation: u64,
    #[serde(skip)]
    pub state: PeerState,
}

impl PeerInner {
    pub fn new(address: Address, incarnation: u64) -> Self {
        PeerInner {
            address,
            incarnation,
            state: PeerState::default(),
        }
    }
    pub fn address(&self) -> &Address {
        &self.address
    }
}

impl Display for PeerInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.address)
    }
}

pub type PeerId = Id<Uuid>;

impl From<Peer> for (PeerId, PeerInner) {
    fn from(peer: Peer) -> Self {
        (peer.0, peer.1)
    }
}
