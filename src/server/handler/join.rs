use std::sync::Arc;

use quinn::SendStream;
use tracing::error;

use crate::cluster::Context;
use crate::message::{Message, Multicast};
use crate::node::Peer;
use crate::{ErrorKind, Result};

pub async fn handle(ctx: Arc<Context>, send: &mut SendStream, peer: Peer) -> Result<()> {
    // respond with membership list
    let peers = {
        let peers = ctx.node().peers().read();
        peers.iter().map(|(id, p)| Peer::from((id, p))).collect()
    };

    let message = Message::Membership(peers);

    match message.write(send).await {
        Ok(_) => {}
        Err(crate::message::Error::StreamWrite(quinn::WriteError::Stopped(e))) => {
            error!("failed to send message; stream is closed; {}", e);
            // TODO: Return indication that stream is closed
            // break;
            return Err(quinn::WriteError::Stopped(e).into());
        }
        Err(e) => error!("failed to send message; {}", e),
    }

    match ctx.add_peer(peer) {
        Err(ErrorKind::KnownMember(_)) => {
            // don't gossip if it is a known member
        }
        Err(e) => return Err(e.into()),
        Ok(peer) => {
            // gossip about new join
            ctx.gossip_tx()
                .send(Multicast::with_payload(Message::Join(peer)))
                .unwrap();
        }
    }

    Ok(())
}
