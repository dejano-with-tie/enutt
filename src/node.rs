use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Id, IdGen};

/// Represents `Node` address by which it can be reached over the network.
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

pub type NodeId = Id<Uuid>;
/// Current node with all cluster members
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    // has to be behind lock as well; what will happened if contact address change because of NAT?
    address: Address,
    incarnation: Mutex<IdGen>,
}

impl Node {
    pub fn new(id: NodeId, address: Address) -> Self {
        Node {
            id,
            address,
            incarnation: Default::default(),
        }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }
    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn inc_incarnation(&self) {
        self.incarnation.lock().next();
    }
    pub fn incarnation(&self) -> u64 {
        self.incarnation.lock().0
    }
}
