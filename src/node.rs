use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Id};

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
    type Error = crate::Error;

    fn try_from(value: &Address) -> Result<Self, Self::Error> {
        value
            .0
            .parse::<SocketAddr>()
            .map_err(|e| Error::Unexpected(e.to_string()))
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

impl From<(PeerId, PeerInner)> for Peer {
    fn from(t: (PeerId, PeerInner)) -> Self {
        Peer(t.0, t.1)
    }
}

impl From<Peer> for (PeerId, PeerInner) {
    fn from(peer: Peer) -> Self {
        (peer.0, peer.1)
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
}

impl Node {
    pub fn new(id: NodeId, address: Address) -> Self {
        Node { id, address }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }
    pub fn address(&self) -> &Address {
        &self.address
    }
}
