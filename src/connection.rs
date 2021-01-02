use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::AsyncWriteExt;
use parking_lot::Mutex;
use quinn::{RecvStream, SendStream};
use tracing::{debug, info, instrument, warn};

use crate::message::Message;
use crate::{Error, IdGen};

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

// Unique key identifying a connection. Two connections will always have distinct keys even if they
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

/// TODO: Docs, streams are super cheap to recreate, that why we don't hold em
#[derive(Clone)]
pub struct ConnectionHolder {
    connection: quinn::Connection,
    remover: ConnectionRemover,
}

impl ConnectionHolder {
    #[instrument(name = "client-connection", skip(connection, remover))]
    pub fn new(connection: quinn::Connection, remover: ConnectionRemover) -> Self {
        Self {
            connection,
            remover,
        }
    }

    fn remove_on_err<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            self.remover.remove();
            // self.close();
        }

        result.map_err(|e| e.into())
    }

    fn remove_on_err_1<T>(&self, result: Result<T, crate::message::Error>) -> Result<T, Error> {
        match result {
            Err(crate::message::Error::ConnectionClosed(e)) => {
                self.close();
                return Err(e.into());
            }
            Err(_) => self.remover.remove(),
            Ok(ref _r) => {}
        };

        result.map_err(|e| e.into())
    }

    /// Gracefully close connection immediately
    pub fn close(&self) {
        self.remover.remove();
        self.connection.close(0u32.into(), b"");
    }

    pub async fn send_bi(&mut self, message: &Message) -> crate::Result<(SendStream, RecvStream)> {
        let (mut send, recv) = self.remove_on_err(self.connection.open_bi().await)?;

        self.write(message, &mut send).await?;

        Ok((send, recv))
    }

    pub async fn send_uni(&mut self, message: &Message) -> crate::Result<()> {
        let mut send = self.remove_on_err(self.connection.open_uni().await)?;

        self.write(message, &mut send).await?;

        self.remove_on_err(send.finish().await)?;

        Ok(())
    }

    async fn write(&mut self, message: &Message, send: &mut SendStream) -> crate::Result<()> {
        if let Err(crate::message::Error::ConnectionClosed(e)) = message.write(send).await {
            self.close();
            return Err(e.into());
        }

        Ok(())
    }
}
