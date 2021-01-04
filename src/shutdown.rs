use std::sync::Arc;

use tokio::sync::oneshot::{Receiver, Sender};
use tracing::{info, info_span};
use tracing_futures::Instrument;

use crate::cluster::Shared;
use crate::membership::Peer;
use crate::message::{Message, Multicast};
use tokio::sync::broadcast;

/// Listens for app shutdown signal.
pub struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,
    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `oneshot::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            shutdown: false,
            notify,
        }
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(mut self) {
        if self.shutdown {
            return;
        }

        let _ = self.notify.recv().await;

        self.shutdown = true;

        // self.ctx
        //     .gossip()
        //     .queue(Multicast::with_payload(Message::Leave(Peer::from(
        //         self.ctx.node().as_ref(),
        //     ))));

        // random sleep, maybe later on provide a gossip response
        // tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;

        // TODO: Persist HQ peers

        info!("graceful shutdown");
    }
}
