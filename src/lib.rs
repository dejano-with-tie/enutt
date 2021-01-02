use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use membership::Peer;

pub mod client;
pub mod cluster;
pub mod config;
pub mod connection;
pub mod gossip;
pub mod membership;
pub mod message;
pub mod node;
pub mod server;
pub mod shutdown;

/// Result type alias
pub type Result<T, E = Error> = std::result::Result<T, E>;
/// Generic Error alias

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Unexpected(String),
    #[error("{0}")]
    UnexpectedSrc(#[from] Box<dyn std::error::Error + 'static + Send + Sync>),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Cluster bootstrap failed")]
    Bootstrap,
    #[error("Configuration: {0}")]
    Configuration(String),
    #[error("Failed to shutdown gracefully")]
    Shutdown,

    #[error("{0}")]
    MessageIo(#[from] crate::message::Error),

    #[error("Establishing connection: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("Connection lost:{0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("Creating endpoint: {0}")]
    Endpoint(#[from] quinn::EndpointError),
    #[error("Stream write/close: {0}")]
    StreamWrite(#[from] quinn::WriteError),
}

#[derive(Default, Debug)]
pub struct IdGen(u64);

impl IdGen {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct Id<T> {
    inner: T,
}

impl Default for Id<Uuid> {
    fn default() -> Self {
        Self {
            inner: Uuid::new_v4(),
        }
    }
}

impl Display for Id<Uuid> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // this is terrible but let it be, im terrible person
        let string = self.inner.to_string();
        let str = string.as_str();

        let len = str.len();
        // Safety: we know the size of `Uuid`
        let first_part = unsafe { str.get_unchecked(0..4) };
        let second_part = unsafe { str.get_unchecked(len - 4..len) };

        write!(f, "{}-{}", first_part, second_part)
    }
}

struct SliceDisplay<'a, T: 'a>(&'a [T]);

impl<'a, T: std::fmt::Display + 'a> std::fmt::Display for SliceDisplay<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut first = true;
        for item in self.0 {
            if !first {
                write!(f, ", {}", item)?;
            } else {
                write!(f, "{}", item)?;
            }
            first = false;
        }
        Ok(())
    }
}
