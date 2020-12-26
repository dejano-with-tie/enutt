use std::sync::Arc;

use quinn::SendStream;
use tracing::debug;

use crate::cluster::Shared;
use crate::message::{Message, Multicast};
use crate::node::Peer;
use crate::{Error, Result};

pub fn handle(ctx: Arc<Shared>, peer: Peer) -> Result<Option<Message>> {
    match ctx.membership().add(peer) {
        Ok(peer) => {
            // gossip about new join
            ctx.gossip()
                .queue(Multicast::with_payload(Message::Join(peer)));
        }
        Err(Error::KnownMember(peer)) => {
            debug!("known member trying to join: {}", peer);
            // don't gossip if it is a known member
        }
        Err(e) => return Err(e.into()),
    }

    Ok(Some(Message::Membership(ctx.membership().peers_clone())))
}
