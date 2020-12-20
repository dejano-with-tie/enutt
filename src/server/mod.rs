use std::borrow::{Borrow, BorrowMut};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::Either::*;
use futures::{StreamExt, TryFutureExt};
use quinn::{Connecting, Connection, Endpoint, Incoming, SendStream, ServerConfig};
use tracing::{debug, error, info, info_span, instrument, warn};
use tracing_futures::Instrument as _;

use crate::cluster::Context;
use crate::message::{Message, Multicast};
use crate::node::Peer;
use crate::ErrorKind;

use super::Result;

pub mod handler;

#[instrument(name = "server", skip(ctx, cfg))]
pub async fn run(ctx: Arc<Context>, cfg: ServerConfig) -> Result<()> {
    let mut endpoint = Endpoint::builder();
    endpoint.listen(cfg);

    let address = SocketAddr::try_from(ctx.node().address()).map_err(|e| {
        error!("unable to parse ip addr; source = {source}", source = e);
        ErrorKind::BootstrapFailure
    })?;

    let (endpoint, mut incoming) = match endpoint.bind(&address) {
        Ok(v) => v,
        Err(e) => {
            error!(?e);
            return Err(ErrorKind::Configuration(format!(
                "start failed; unable to bind to [{}]",
                &address
            ))
            .into());
        }
    };
    // only receive
    // drop(endpoint);

    let mut server = Server {
        ctx,
        endpoint,
        incoming,
    };

    let server_task = async move {
        server.run().await;
    };
    tokio::spawn(server_task.in_current_span());

    info!("started: {}", address);

    Ok(())
}

struct Server {
    ctx: Arc<Context>,
    endpoint: Endpoint,
    incoming: Incoming,
}

impl Server {
    async fn run(&mut self) {
        while let Some(connection) = self.incoming.next().await {
            let addr = connection.remote_address();

            let mut conn_handler = ConnectionHandler {
                ctx: Arc::clone(&self.ctx),
                connection,
            };

            let connection_span = info_span!("connection",addr = %addr);
            let connection_task = async move {
                if let Err(e) = conn_handler.handle_connection().await {
                    error!("failed: {reason}", reason = e)
                }
            };
            tokio::spawn(connection_task.instrument(connection_span));

            // tokio::spawn(async move {
            //     self.handle_connection(connection)
            //         .unwrap_or_else(move |e| error!("failed: {reason}", reason = e))
            //         .instrument(connection_span);
            // });
        }
    }
}

struct ConnectionHandler {
    ctx: Arc<Context>,
    connection: Connecting,
}

impl ConnectionHandler {
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
                        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                            debug!("terminated by peer");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                        Ok(s) => s,
                    };
                    (None, recv)
                }
                else => return Ok(())
            };

            let request_handler = RequestHandler {
                ctx: Arc::clone(&self.ctx),
            };
            let request_task = async move {
                if let Err(e) = request_handler
                    .handle_request(send_stream, recv_stream)
                    .await
                {
                    error!("failed: {reason}", reason = e);
                }
            };

            tokio::spawn(request_task.instrument(info_span!("request")));
        }
    }
}

struct RequestHandler {
    ctx: Arc<Context>,
}

impl RequestHandler {
    async fn handle_request(
        &self,
        mut send: Option<quinn::SendStream>,
        mut recv: quinn::RecvStream,
    ) -> Result<()> {
        loop {
            match Message::read(&mut recv).await {
                Ok(Some(request)) => {
                    info!("incoming: {}", request);
                    let either = match send {
                        None => Left(self.handle_uni_request(request, &mut recv)),
                        Some(ref mut send) => {
                            Right(self.handle_bi_request(request, send, &mut recv))
                        }
                    };
                    either.await?;
                }
                Ok(None) => {
                    // info!("stream closed by peer");
                    // don't read it anymore
                    return Ok(());
                }
                // close the stream (and not the connection itself, there might be more valid streams?)
                Err(e) => return Err(e.into()),
            };
        }
    }

    async fn handle_uni_request(
        &self,
        request: Message,
        _recv: &mut quinn::RecvStream,
    ) -> Result<()> {
        match request {
            Message::Multicast(multicast) => {
                let payload = multicast.payload().clone();
                self.ctx.gossip_tx().send(multicast).unwrap();

                if let Message::Join(peer) = payload {
                    match self.ctx.add_peer(peer) {
                        _ => {
                            // let node = self.ctx.node();
                            // let peers = node.peers().read();
                            // println!("{} -> {:?}", node.address(), peers);
                        }
                    };
                }
            }
            _ => warn!("dunno how to handle this message"),
        }
        Ok(())
    }

    async fn handle_bi_request(
        &self,
        request: Message,
        send: &mut quinn::SendStream,
        _recv: &mut quinn::RecvStream,
    ) -> Result<()> {
        match request {
            Message::Join(peer) => {
                handler::join::handle(Arc::clone(&self.ctx), send, peer).await?;
            }
            _ => warn!("dunno how to handle this message"),
        }
        Ok(())
    }
}
