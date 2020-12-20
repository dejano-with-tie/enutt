use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{ErrorKind, Id};
use uuid::Uuid;

/// Represents _node_ address by which it can be reached over the network.
/// Atm it is just a string but later on should be expanded to contain necessary info
/// how to reach node over the network
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct Address(pub String);

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&Address> for SocketAddr {
    type Error = crate::ErrorKind;

    fn try_from(value: &Address) -> Result<Self, Self::Error> {
        value
            .0
            .parse::<SocketAddr>()
            .map_err(|e| ErrorKind::Unexpected(e.to_string()))
    }
}

pub type PeerId = Id<Uuid>;
/// Represents arbitrary _node_ in a cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Peer(PeerId, PeerInner);

impl Peer {
    pub fn new(address: Address, uuid: PeerId) -> Self {
        Peer(uuid, PeerInner::new(address))
    }

    pub fn id(&self) -> &PeerId {
        &self.0
    }

    pub fn inner(&self) -> &PeerInner {
        &self.1
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Peer({}, {})", self.0, self.1)
    }
}

impl From<&Node> for Peer {
    fn from(node: &Node) -> Self {
        Peer::new(node.address.clone(), node.id)
    }
}

impl From<(&PeerId, &PeerInner)> for Peer {
    fn from(entry: (&PeerId, &PeerInner)) -> Self {
        Peer(*entry.0, entry.1.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInner {
    address: Address,
}

impl PeerInner {
    pub fn new(address: Address) -> Self {
        PeerInner { address }
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

pub type NodeId = Id<Uuid>;
/// Current node with all cluster members
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    // has to be behind lock as well; what will happened if contact address change because of NAT?
    address: Address,
    /// TODO: Dedicate separate struct for peers (membership)
    peers: RwLock<HashMap<NodeId, PeerInner>>,
}

impl Node {
    pub fn new(id: NodeId, address: Address, peers: HashMap<PeerId, PeerInner>) -> Self {
        let peers = RwLock::new(peers);

        let mut w = peers.write();
        w.insert(id, PeerInner::new(address.clone()));
        drop(w);

        Node { id, address, peers }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn address(&self) -> &Address {
        &self.address
    }
    pub fn peers(&self) -> &RwLock<HashMap<PeerId, PeerInner>> {
        &self.peers
    }
}
