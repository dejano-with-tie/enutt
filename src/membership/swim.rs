use std::collections::HashSet;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;

use crate::client::Client;
use crate::cluster::Shared;
use crate::membership::Peer;
use crate::message::{Message, Multicast};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("probe timeout")]
    ProbeTimeout,
}

/// Start swim protocol
///
/// Spawn two background tasks
/// - swim protocol task
/// - failure dissemination task
pub fn run(cfg: &crate::config::Swim, shared: Arc<Shared>) {
    let share_fail = shared.clone();

    let mut swim = Swim {
        cfg: cfg.clone(),
        rtt: None,
        shared,
    };

    tokio::spawn(disseminate_failure(share_fail));
    tokio::spawn(
        async move {
            swim.run().await;
        }
        .instrument(info_span!("swim")),
    );
}

/// Listen for membership component to announce peer failure and spread this info to the cluster
async fn disseminate_failure(ctx_failed: Arc<Shared>) {
    let mut failed = ctx_failed.membership().failed.subscribe();
    loop {
        if let Ok(failed) = failed.recv().await {
            ctx_failed.client().close(failed.inner().address());
            ctx_failed
                .gossip()
                .queue(Multicast::with_payload(Message::Failed(failed)));
        }
    }
}

pub struct Swim {
    cfg: crate::config::Swim,
    /// calculated average rtt. Not used atm
    rtt: Option<u64>,
    shared: Arc<Shared>,
}

impl Swim {
    fn rtt(&self) -> Duration {
        Duration::from_millis(self.rtt.unwrap_or_else(|| self.cfg.initial_rtt()))
    }

    fn protocol_period(&self) -> Duration {
        (self.cfg.period_rate() as u32) * self.rtt()
    }

    /// Swim protocol loop
    ///
    /// Swim cycle is a period in which every known non-failed peer has been probed. It takes
    /// `self.protocol_period()` * `membership_len` time to complete. At the begging of each cycle
    /// peers are shuffled to ensure they are selected in round-robin fashion.
    ///
    /// Every `self.protocol_period()` select one peer to probe.
    pub async fn run(&mut self) {
        let mut protocol_period = tokio::time::interval(self.protocol_period());
        loop {
            debug!("starting swim cycle (Time now = {:?})", Instant::now());
            let shuffled = self.shared.membership().shuffled().into_iter();

            if shuffled.len() == 0 {
                // in case there are no peers yet, to avoid looping
                // can be improved with notify
                debug!("no peers; continue");
                protocol_period.tick().await;
                continue;
            }

            // swim cycle, T*N seconds
            for peer in shuffled {
                self.probe_and_gossip(peer).await;
                protocol_period.tick().await;
            }

            // update rtt
        }
    }

    /// Check is peer still alive and disseminating result to the rest of the cluster.
    ///
    /// This is done in following manner:
    /// - Send `ping` message to peer
    ///     - If peer failed to respond with `ack` within `self.rtt()` time, select random `K` peers
    /// and ask them to do the pinging for you (indirect ping)
    ///     - If indirect pinging fails withing `self.protocol_period()` time, mark peer as `suspected` and disseminate this information
    /// to the rest of the cluster.
    /// - If `ping` is successful (direct or indirect), mark peer as `alive` and disseminate info to the rest of the cluster.
    async fn probe_and_gossip(&self, peer: Peer) {
        info!("probe {}", peer);
        let message = match timeout(self.protocol_period(), self.probe(&peer)).await {
            Ok(Ok(_)) => {
                if !self.shared.membership().is_suspected(peer.id()) {
                    None
                } else {
                    self.shared.membership().alive(peer.clone());
                    Some(Multicast::with_payload(Message::Alive(peer)))
                }
            }
            Err(_) | Ok(Err(_)) => {
                error!("probe failed: {}", peer);
                self.shared.client().close(peer.inner().address());
                self.shared
                    .membership()
                    .suspect(peer)
                    .map(|peer| Multicast::with_payload(Message::Suspect(peer)))
            }
        };

        if let Some(message) = message {
            self.shared.gossip().queue(message);
        }
    }

    async fn probe(&self, to_probe: &Peer) -> crate::Result<(), Error> {
        // NOTE: tokio timeout cancels the ping request and stream is not getting closed gracefully
        let ping_result = match timeout(self.rtt(), ping(self.shared.client(), to_probe)).await {
            Ok(_) => return Ok(()),
            Err(_) => Err(Error::ProbeTimeout),
        };

        let indirect_ping_requests = match self.indirect_ping(to_probe) {
            Some(requests) => requests,
            None => return ping_result,
        };

        futures::future::select_ok(indirect_ping_requests)
            .await
            .map(|_| ())
    }

    fn indirect_ping<'a>(
        &'a self,
        to_ping: &'a Peer,
    ) -> Option<Vec<BoxFuture<'a, Result<(), Error>>>> {
        let mut ignore = HashSet::new();
        ignore.insert(*to_ping.id());

        let peers = match self
            .shared
            .membership()
            .random_samples(self.cfg.k(), &ignore)
        {
            Some(peers) => peers,
            None => return None,
        };

        let client = self.shared.client();
        Some(
            peers
                .into_iter()
                .map(|peer| indirect_ping(client, to_ping, peer).boxed())
                .collect(),
        )
    }
}

pub async fn ping(client: &Client, peer: &Peer) -> crate::Result<(), Error> {
    debug!("ping: {peer}", peer = peer);
    let (_, mut rx) = client
        .send_bi(peer.inner().address(), &Message::Ping)
        .map_err(|_| Error::ProbeTimeout)
        .await?;

    let result = match Message::read(&mut rx).await {
        Ok(None) | Err(_) => Err(Error::ProbeTimeout),
        // Ok(Some(_)) - this means there is response but could be non ACK message
        // theoretically speaking this is possible but shouldn't happen
        // Anyhow, as long as we got something back it means peer is alive
        Ok(Some(Message::Ack)) | Ok(Some(_)) => Ok(()),
    };

    return result;
}

async fn indirect_ping(client: &Client, to_ping: &Peer, peer: Peer) -> crate::Result<(), Error> {
    debug!("try indirect_ping: {} over {}", to_ping, peer);
    let (_, mut rx) = client
        .send_bi(
            peer.inner().address(),
            &Message::IndirectPing((to_ping).clone()),
        )
        .map_err(|_| Error::ProbeTimeout)
        .await?;

    return match Message::read(&mut rx).await {
        Ok(Some(Message::IndirectPingAck(_))) => Ok(()),
        Ok(None) | Err(_) | Ok(Some(_)) => Err(Error::ProbeTimeout),
    };
}

/// If suspected `peer` is `me`, increase incarnation number and
/// return `Some(Alive)` message
///
/// Message which is further disseminated will be changed to
/// value inside returned `Some` or will be the same if `None` is returned.
pub fn handle_suspect_msg(shared: &Arc<Shared>, peer: Peer) -> Option<Multicast> {
    let membership = shared.membership();
    let me = membership.me();
    if me.id() == peer.id() {
        warn!("They are saying i'm failing :O");
        me.inc_incarnation();
        return Some(Multicast::with_payload(Message::Alive(Peer::from(me))));
    }

    membership.suspect(peer);
    None
}
pub fn handle_failed_msg(shared: &Arc<Shared>, peer: Peer) -> Option<Multicast> {
    shared.client().close(peer.inner().address());
    shared.membership().remove(peer.id());
    None
}

pub fn handle_alive_msg(shared: &Arc<Shared>, peer: Peer) -> Option<Multicast> {
    shared.membership().alive(peer);
    None
}
