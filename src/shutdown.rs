use std::sync::Arc;

use crate::cluster::Context;
use crate::message::{Message, Multicast};
use crate::node::Peer;
use crate::ErrorKind;
use tracing::{info_span, info};
use tracing_futures::Instrument;
use tokio::sync::oneshot::{Sender, Receiver};

/// Listens for app shutdown signal.
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,
    ctx: Arc<Context>,
    rx: Receiver<()>,
    result_tx: Sender<Result<(), ErrorKind>>,
}

// TODO: Try to do this with traits
// let each struct interested in this event to implement Shutdown trait
pub async fn listen(ctx: Arc<Context>, notify: Receiver<()>) -> Receiver<Result<(), ErrorKind>> {
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<(), ErrorKind>>();
    
    let shutdown = Shutdown::new(ctx, notify, result_tx);
    
    tokio::spawn(shutdown.on_shutdown().instrument(info_span!("shutdown")));

    result_rx
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `oneshot::Receiver`.
    pub(crate) fn new(ctx: Arc<Context>, rx: Receiver<()>, result_tx: Sender<Result<(), ErrorKind>>) -> Self {
        
        Self {
            shutdown: false,
            ctx,
            rx,
            result_tx,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn on_shutdown(mut self) {
        self.rx.await.unwrap();
        
        self.shutdown = true;
            // TODO: do bi_send leave
        self.ctx
            .gossip()
            .queue(Multicast::with_payload(Message::Leave(Peer::from(
                self.ctx.node().as_ref(),
            ))));

        // random sleep, maybe later on provide a gossip response
        tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;

        // TODO: Persist HQ peers

        // shutdown.notify();
        // tx.send(Ok(()));
        self.result_tx.send(Ok(())).unwrap();
    }
}
