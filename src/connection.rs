use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use quinn::{ClientConfig, Endpoint, RecvStream, SendStream};
use tracing::{debug, info, instrument, warn};
use tracing_futures::Instrument as _;

use crate::message::Message;
use crate::node::Address;

// Pool for keeping open connections. Pooled connections are associated with a `ConnectionRemover`
// which can be used to remove them from the pool.
#[derive(Clone)]
pub(crate) struct ConnectionPool {
    store: Arc<Mutex<Store>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(Store::default())),
        }
    }

    pub fn insert(&self, addr: SocketAddr, conn: quinn::Connection) -> ConnectionRemover {
        let mut store = self.store.lock();

        let key = Key {
            addr,
            id: store.id_gen.next(),
        };
        let _ = store.map.insert(key, conn);

        ConnectionRemover {
            store: self.store.clone(),
            key,
        }
    }

    pub fn get(&self, addr: &SocketAddr) -> Option<(quinn::Connection, ConnectionRemover)> {
        let mut store = self.store.lock();

        // Efficiently fetch the first entry whose key is equal to `key`.
        let (key, conn) = store
            .map
            .range_mut(Key::min(*addr)..=Key::max(*addr))
            .next()?;

        let conn = conn.clone();
        let remover = ConnectionRemover {
            store: self.store.clone(),
            key: *key,
        };

        Some((conn, remover))
    }
}

// Handle for removing a connection from the pool.
#[derive(Clone)]
pub struct ConnectionRemover {
    store: Arc<Mutex<Store>>,
    key: Key,
}

impl ConnectionRemover {
    // Remove the connection from the pool.
    pub fn remove(&self) {
        let mut store = self.store.lock();
        let _ = store.map.remove(&self.key);
    }

    pub fn remote_addr(&self) -> &SocketAddr {
        &self.key.addr
    }
}

#[derive(Default)]
struct Store {
    // TODO: Make it LRU map (+time based?)
    map: BTreeMap<Key, quinn::Connection>,
    id_gen: IdGen,
}

// Unique key identifying a connection. Two connections will always have distict keys even if they
// have the same socket address.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct Key {
    addr: SocketAddr,
    id: u64,
}

impl Key {
    // Returns the minimal `Key` for the given address according to its `Ord` relation.
    fn min(addr: SocketAddr) -> Self {
        Self { addr, id: u64::MIN }
    }

    // Returns the maximal `Key` for the given address according to its `Ord` relation.
    fn max(addr: SocketAddr) -> Self {
        Self { addr, id: u64::MAX }
    }
}

#[derive(Default)]
struct IdGen(u64);

impl IdGen {
    fn next(&mut self) -> u64 {
        let id = self.0;
        self.0 = self.0.wrapping_add(1);
        id
    }
}

/// TODO: Docs, streams are super cheap to recreate, that why we don't hold em
#[derive(Clone)]
pub struct ConnectionHolder {
    connection: quinn::Connection,
    remover: ConnectionRemover,
}

impl ConnectionHolder {
    #[instrument(name = "client-connection", skip(connection, remover))]
    pub fn new(connection: quinn::Connection, remover: ConnectionRemover) -> crate::Result<Self> {
        Ok(Self {
            connection,
            remover,
        })
    }

    fn remove_on_err<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            self.remover.remove()
        }
        result
    }

    /// Gracefully close connection immediatelly
    pub fn close(&self) {
        self.connection.close(0u32.into(), b"");
        self.remover.remove();
    }

    pub async fn send_bi(&mut self, message: &Message) -> crate::Result<(SendStream, RecvStream)> {
        // we called connect_to before this so it is alright to unwrap
        let (mut send, recv) = self.remove_on_err(self.connection.open_bi().await)?;

        message.write(&mut send).await?;

        if send.finish().await.is_err() {
            warn!("failed to finish stream gracefully");
        };

        // self.endpoint.wait_idle().await;

        Ok((send, recv))
    }
    pub async fn send_uni(&mut self, message: &Message) -> crate::Result<SendStream> {
        let mut send = self.remove_on_err(self.connection.open_uni().await)?;

        message.write(&mut send).await?;

        if send.finish().await.is_err() {
            warn!("failed to finish stream gracefully");
        };

        // self.endpoint.wait_idle().await;

        Ok(send)
    }
}
