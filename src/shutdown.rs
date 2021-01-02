use std::sync::Arc;

use tokio::sync::oneshot::{Receiver, Sender};
use tracing::{info, info_span};
use tracing_futures::Instrument;

use crate::cluster::Shared;
use crate::membership::Peer;
use crate::message::{Message, Multicast};
use crate::Error;
use tokio::sync::Notify;

/// Listens for app shutdown signal.
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,
    ctx: Arc<Shared>,
    notify: Arc<Notify>,
}

// TODO: Try to do this with traits
// let each struct interested in this event to implement Shutdown trait
pub async fn listen(ctx: Arc<Shared>, notify: Arc<Notify>) {
    let shutdown = Shutdown::new(ctx, notify);
    tokio::spawn(shutdown.on_shutdown().instrument(info_span!("shutdown")));
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `oneshot::Receiver`.
    pub(crate) fn new(ctx: Arc<Shared>, notify: Arc<Notify>) -> Self {
        Self {
            shutdown: false,
            ctx,
            notify,
        }
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn on_shutdown(mut self) {
        self.notify.notified().await;

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
