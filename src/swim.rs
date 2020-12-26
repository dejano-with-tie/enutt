use std::collections::HashSet;
use std::sync::Arc;

use futures::future::{AbortHandle, Abortable, Aborted, BoxFuture, Either};
use futures::{FutureExt, StreamExt};
use tokio::time::Duration;
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

use crate::client::Client;
use crate::cluster::Shared;
use crate::message::{Message, Multicast};
use crate::node::Peer;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Ping timeout")]
    Timeout,
    #[error("Ping timeout")]
    Aborted(#[from] Aborted),
    #[error("Ping failed")]
    Connection(#[from] crate::Error),
}

pub fn run(cfg: &crate::config::Swim, ctx: Arc<Shared>) {
    let mut swim = Swim {
        period: 6 * cfg.initial_rtt(),
        time_out: 2 * cfg.initial_rtt(),
        initial_rtt: cfg.initial_rtt(),
        rtt: None,
        ping_req_fanout: cfg.k(),
        ctx,
    };

    tokio::spawn(async move {
        swim.run().instrument(info_span!("swim")).await;
    });
}

pub struct Swim {
    initial_rtt: u64,
    rtt: Option<u64>,
    period: u64,
    time_out: u64,
    ping_req_fanout: usize,
    ctx: Arc<Shared>,
}

impl Swim {
    pub fn rtt(&self) -> u64 {
        self.rtt.unwrap_or(self.initial_rtt)
    }

    pub fn time_out(&self) -> u64 {
        2 * self.rtt.unwrap_or(self.initial_rtt)
    }

    // ping

    // response before TO period?
    // // true
    // // -> alive

    // // false
    // // (Optional) ask gossip layer for a message to piggy back (PBM)
    // // -> send ping-req to K random peers ping-req(K[i], unresponsive-peer) with piggy back

    /////// response before T period?

    /////// true -> alive
    /////// false -> suspect
    pub async fn run(&mut self) {
        let mut to_ignore = HashSet::new();
        to_ignore.insert(self.ctx.node().id());

        let mut period_interval =
            tokio::time::interval(std::time::Duration::from_millis(self.period));
        loop {
            // swim cycle, T*N seconds
            let mut shuffled = self.ctx.membership().shuffled();
            shuffled.retain(|peer| peer.id() != self.ctx.node().id());
            let mut iter = shuffled.into_iter();

            if iter.len() == 0 {
                // not the best way to do it but works for now
                period_interval.tick().await;
                continue;
            }

            while let Some(peer) = iter.next() {
                info!("PROBE {}", peer);
                // swim protocol period, T seconds
                let now = period_interval.tick().await;
                debug!("starting period (Time now = {:?})", now);

                let probe_result = tokio::select! {
                    result = self.probe(&peer) => {
                        result
                    }
                    // give it 100 milliseconds gap
                    _ = tokio::time::delay_for(Duration::from_millis(self.period - 100)) => { Err(Error::Timeout)}
                };

                let message = match probe_result {
                    Err(e) => {
                        info!("Probe failed; {reason}", reason = e);
                        self.ctx.membership().remove(&peer);
                        Multicast::with_payload(Message::Suspect(peer))
                    }
                    Ok(_) => Multicast::with_payload(Message::Alive(peer)),
                };
                self.ctx.gossip().queue(message);
            }

            info!("completed swim cycle");

            // update interval

            // update period
            if let Some(rtt) = self.rtt {
                self.period = 6 * rtt;
                period_interval =
                    tokio::time::interval(std::time::Duration::from_millis(self.period));
            }
        }
    }

    async fn probe(&self, to_probe: &Peer) -> crate::Result<(), Error> {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let ping = Abortable::new(ping(self.ctx.client(), to_probe), abort_registration);

        let ping_result: Result<(), Error> = tokio::select! {
            ping_result = ping => { ping_result.unwrap_or_else(|e| Err(e.into())) }
            _ = tokio::time::delay_for(Duration::from_millis(self.time_out())) => { Err(Error::Timeout) }
        };
        let ping_result = match ping_result {
            Ok(_) => return Ok(()),
            Err(e) => {
                abort_handle.abort();
                Err(e)
            }
        };

        let ping_reqs = match self.ping_req(to_probe) {
            Some(ping_reqs) => ping_reqs,
            None => return ping_result,
        };

        futures::future::select_ok(ping_reqs).await.map(|_| ())
    }

    fn ping_req<'a>(&'a self, to_ping: &'a Peer) -> Option<Vec<BoxFuture<'a, Result<(), Error>>>> {
        let mut to_ignore = HashSet::new();
        to_ignore.insert(*self.ctx.node().id());
        // try with ping_req
        // get K other members
        let peers = match self
            .ctx
            .membership()
            .random(self.ping_req_fanout, &to_ignore)
        {
            Some(peers) => peers,
            None => return None,
        };

        let client = self.ctx.client();
        Some(
            peers
                .into_iter()
                .map(|peer| ping_req(client, to_ping, peer).boxed())
                .collect(),
        )
    }
}

pub async fn ping(client: &Client, peer: &Peer) -> crate::Result<(), Error> {
    info!("try ping: {peer}", peer = peer);
    let (_, mut rx) = client
        .send_bi(peer.inner().address(), &Message::Ping)
        .await?;

    let result = match Message::read(&mut rx).await {
        Err(_) | Ok(None) => Err(Error::Timeout),
        Ok(Some(Message::Ack)) => Ok(()),
        Ok(Some(_)) => {
            // this means there is response but could be non ACK message
            // theoretically speaking this is possible but shouldn't happen
            // Anyhow, as long as we got something back it means peer is alive
            Ok(())
        }
    };

    return result;
}

async fn ping_req(client: &Client, to_ping: &Peer, peer: Peer) -> crate::Result<(), Error> {
    info!("try request_ping: {peer}", peer = peer);
    let (_, mut rx) = client
        .send_bi(peer.inner().address(), &Message::PingReq((to_ping).clone()))
        .await?;

    return match Message::read(&mut rx).await {
        Err(e) => {
            error!("read ping response: {reason}", reason = e);
            Err(Error::Timeout)
        }
        Ok(None) => Err(Error::Timeout),
        Ok(Some(Message::PingReqAck(_peer))) => Ok(()),
        Ok(Some(_)) => {
            // same case as in ping()
            Ok(())
        }
    };
}
