use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Either::*;
use futures::{AsyncWriteExt, StreamExt};
use quinn::{Connecting, Endpoint, Incoming, ServerConfig};
use tracing::{debug, error, info, info_span, instrument, warn};
use tracing_futures::Instrument as _;

use crate::client::Client;
use crate::cluster::Shared;
use crate::membership::{swim, Peer};
use crate::message::{Message, Multicast};
use crate::Error;

use super::Result;

#[instrument(name = "server", skip(ctx, cfg))]
pub async fn run(ctx: Arc<Shared>, cfg: ServerConfig) -> Result<()> {
    let mut endpoint = Endpoint::builder();
    endpoint.listen(cfg);

    let address = SocketAddr::try_from(ctx.membership().me().address()).map_err(|e| {
        error!("unable to parse ip addr; source = {source}", source = e);
        Error::BootstrapFailure
    })?;

    let (endpoint, incoming) = match endpoint.bind(&address) {
        Ok(v) => v,
        Err(e) => {
            error!(?e);
            return Err(Error::Configuration(format!(
                "start failed; unable to bind to [{}]",
                &address
            )));
        }
    };

    let mut server = Server {
        ctx,
        endpoint,
        incoming,
    };

    tokio::spawn(
        async move {
            server.run().await;
        }
        .in_current_span(),
    );

    info!("started: {}", address);

    Ok(())
}

struct Server {
    ctx: Arc<Shared>,
    endpoint: Endpoint,
    incoming: Incoming,
}

impl Server {
    async fn run(&mut self) {
        while let Some(connection) = self.incoming.next().await {
            let remote_addr = connection.remote_address();

            let mut conn_handler = ConnectionHandler {
                ctx: Arc::clone(&self.ctx),
                connection,
            };

            tokio::spawn(
                async move {
                    if let Err(e) = conn_handler.handle_connection().await {
                        error!("{addr} failed: {reason}", addr = remote_addr, reason = e)
                    }
                }
                .in_current_span(),
            );
        }
    }
}

struct ConnectionHandler {
    ctx: Arc<Shared>,
    connection: Connecting,
}

impl ConnectionHandler {
    #[instrument(name = "connection", skip(self), fields(addr = %self.connection.remote_address()))]
    async fn handle_connection(&mut self) -> Result<()> {
        let connection = self.connection.borrow_mut().await?;
        let quinn::NewConnection {
            connection: _,
            mut bi_streams,
            mut uni_streams,
            ..
        } = connection;

        debug!("established");

        loop {
            let (send_stream, recv_stream) = tokio::select! {
                Some(stream) = bi_streams.next() => {
                    let (send, recv) = match stream {
                        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                            debug!("terminated by peer");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                        Ok(s) => s,
                    };
                    (Some(send), recv)
                },
                Some(stream) = uni_streams.next() => {
                     let recv = match stream {
                        Ok(s) => s,
                        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                            debug!("terminated by peer");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    };
                    (None, recv)
                }
                else => return Ok(())
            };

            let request_handler = RequestHandler {
                ctx: Arc::clone(&self.ctx),
            };

            tokio::spawn(
                async move {
                    if let Err(e) = request_handler
                        .handle_request(send_stream, recv_stream)
                        .await
                    {
                        error!("failed: {reason}", reason = e);
                    }
                }
                .instrument(info_span!("request")),
            );
        }
    }
}

struct RequestHandler {
    ctx: Arc<Shared>,
}

impl RequestHandler {
    async fn handle_request(
        &self,
        mut send: Option<quinn::SendStream>,
        mut recv: quinn::RecvStream,
    ) -> Result<()> {
        loop {
            let message = match Message::read(&mut recv).await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    debug!("stream closed by peer");
                    return Ok(());
                }
                // close the stream
                Err(e) => return Err(e.into()),
            };

            debug!("incoming: {}", message);
            match send {
                Some(ref mut send) => Right(self.handle_bi_request(message, send)),
                None => Left(self.handle_uni_request(message)),
            }
            .await?;
        }
    }

    /// Any error returned will break the current stream but not the connection itself
    async fn handle_uni_request(&self, message: Message) -> Result<()> {
        let multicast = match message {
            Message::Disseminate(multicast) => multicast,
            _ => {
                error!("unknown message: {}", message);
                return Ok(());
            }
        };

        let payload = multicast.payload().clone();

        let multicast = match payload {
            Message::Join(peer) => {
                self.ctx.membership().insert(peer);
                Some(multicast)
            }
            Message::Leave(peer) => {
                self.ctx.membership().remove(peer.id());
                Some(multicast)
            }
            Message::Suspect(peer) => {
                Some(swim::handle_suspect_msg(&self.ctx, peer).unwrap_or(multicast))
            }
            Message::Alive(peer) => {
                Some(swim::handle_alive_msg(&self.ctx, peer).unwrap_or(multicast))
            }
            Message::Failed(peer) => {
                Some(swim::handle_failed_msg(&self.ctx, peer).unwrap_or(multicast))
            }

            _ => Some(multicast),
        };

        if let Some(multicast) = multicast {
            self.ctx.gossip().queue(multicast);
        }

        Ok(())
    }

    /// Any error returned will break the current stream but not the connection itself
    async fn handle_bi_request(
        &self,
        request: Message,
        send: &mut quinn::SendStream,
    ) -> Result<()> {
        let response = match request {
            Message::Join(peer) => handle_join(&self.ctx, peer)?,
            Message::Ping => Some(Message::Ack),
            Message::IndirectPing(peer) => handle_indirect_ping(self.ctx.client(), peer).await,
            _ => {
                error!("received UNKNOWN message: {message}", message = request);
                return Ok(());
            }
        };

        // not sure if it is good idea to finish send part of the stream in bi directional stream when response is `None`
        if let Some(response) = response {
            response.write(send).await?
        }

        Ok(())
    }
}

/// If joining peer is unknown, disseminate `Join` message
///
/// Return membership list to joining peer
pub fn handle_join(shared: &Arc<Shared>, peer: Peer) -> Result<Option<Message>> {
    if let Some(peer) = shared.membership().insert(peer) {
        shared
            .gossip()
            .queue(Multicast::with_payload(Message::Join(peer)));
    }

    Ok(Some(Message::Membership(shared.membership().peers_clone())))
}

async fn handle_indirect_ping(client: &Client, peer: Peer) -> Option<Message> {
    match swim::ping(client, &peer).await {
        Ok(_) => Some(Message::IndirectPingAck(peer)),
        Err(_) => None,
    }
}
