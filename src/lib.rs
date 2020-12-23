use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::node::Peer;

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
pub mod swim;

/// Error and Result type alias
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum ErrorKind {
    #[error("{0}")]
    Unexpected(String),
    #[error("{0}")]
    UnexpectedSrc(Error),
    #[error("Cluster bootstrap failed")]
    BootstrapFailure,
    #[error("Configuration: {0}")]
    Configuration(String),
    #[error("Known member: {0:?}")]
    KnownMember(Peer),
    #[error("Failed to shutdown gracefully")]
    ShutdownFailure,
}

impl From<Error> for ErrorKind {
    fn from(e: Error) -> Self {
        ErrorKind::UnexpectedSrc(e)
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
        // SAFETY: we know the size of `Uuid`
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_display() {
        let id = Id::default();
        println!("{}", id);
    }
}
