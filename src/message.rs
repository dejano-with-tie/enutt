use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};

use byte::ctx::Endian;
use byte::{BytesExt, TryRead, TryWrite};
use derive_more::Display;
use futures::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::node::{Peer, PeerId};
use crate::{Id, SliceDisplay};

#[derive(Error, Debug)]
pub enum Error {
    #[error("expected header size: {0}; actual header: {1:?}")]
    HeaderWrite(u32, MessageHeader),
    #[error("[{0:?}] {0}")]
    HeaderRead(crate::Error),

    #[error("(De)Serialization failed; [{0:?}] {0}")]
    DeSerialize(#[from] bincode::Error),

    #[error("Failed to write to stream; [{0:?}] {0}")]
    StreamWrite(#[from] quinn::WriteError),
    #[error("Failed to read from stream; [{0:?}] {0}")]
    StreamRead(#[from] quinn::ReadExactError),

    #[error("[{0:?}] {0}")]
    Other(crate::Error),
}

/// Total length of message header in bytes
const HEADER_MESSAGE_LEN: usize = 9;

/// Protocol current version
const MSG_PROTOCOL_VERSION: u8 = 0x0001;

/// Message Header that is sent over the wire
/// Format of the message header is as follows
/// | version | message type   | message length | reserved |
/// | 1 byte  |     2 bytes    |    4 byte      | 2 bytes  |
#[derive(Serialize, Deserialize, Debug, Clone, Display)]
#[display(fmt = "({}, {}, {}, {:?})", version, id, len, reserved)]
pub struct MessageHeader {
    /// Protocol version
    version: u8,
    /// Unique identifier for specific message (payload)
    id: u16,
    /// Length of payload
    len: usize,
    /// Reserved for future use
    #[allow(unused)]
    reserved: [u8; 2],
}

/// Messages that are sent over the wire
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    Join(Peer),
    Leave(Peer),
    Ping,
    Membership(Vec<Peer>),
    Multicast(Multicast),
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Join(peer) => {
                write!(f, "[{}]({})", self.name(), peer.inner().address())
            }
            Message::Leave(peer) => {
                write!(f, "[{}]({})", self.name(), peer.inner().address())
            }
            Message::Ping => {
                write!(f, "[{}]", self.name())
            }
            Message::Membership(peers) => {
                write!(f, "[{}]({})", self.name(), SliceDisplay(peers, &true))
            }
            Message::Multicast(multicast) => {
                write!(
                    f,
                    "[{}](id: {}, payload: {})",
                    self.name(),
                    multicast.id(),
                    *multicast.payload
                )
            }
        }
    }
}

/// Multicast message with meta data and payload
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Multicast {
    inner: MulticastInner,
    payload: Box<Message>,
}

impl Display for Multicast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Multicast (id: {}, payload: {})",
            self.id(),
            *self.payload
        )
    }
}

impl Multicast {
    pub fn new(inner: MulticastInner, payload: Message) -> Self {
        Self {
            inner,
            payload: Box::new(payload),
        }
    }

    pub fn with_payload(payload: Message) -> Self {
        Self::new(MulticastInner::default(), payload)
    }

    pub fn payload(&self) -> &Message {
        &self.payload
    }

    pub fn id(&self) -> &MulticastId {
        &self.inner.id
    }
    pub fn infected(&self) -> &HashSet<Id<Uuid>> {
        &self.inner.infected
    }

    pub fn insert_infected(&mut self, peer_id: PeerId) {
        self.inner.infected.insert(peer_id);
    }
}

pub type MulticastId = Id<Uuid>;

/// Meta information about multicast message
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MulticastInner {
    // TODO: Add date time here
    /// ID which is used to determine if message has been seen already by current node
    id: MulticastId,
    /// Set of peer ids that has seen this multicast
    /// Message won't be sent to peers in this list
    infected: HashSet<PeerId>,
    /// gossip round counter
    hop_count: usize,
}

impl Default for MulticastInner {
    fn default() -> Self {
        Self {
            id: MulticastId::default(),
            infected: HashSet::new(),
            hop_count: 0,
        }
    }
}

impl Message {
    fn id(&self) -> u16 {
        match self {
            Message::Join(_) => 1,
            Message::Leave(_) => 2,
            Message::Ping => 3,
            Message::Membership(_) => 4,
            Message::Multicast(_) => 5,
        }
    }
    /// For displaying purposes
    fn name(&self) -> &'static str {
        match self {
            Message::Join(_) => "JOIN",
            Message::Leave(_) => "LEAVE",
            Message::Ping => "PING",
            Message::Membership(_) => "MEMBERSHIP",
            Message::Multicast(_) => "MULTICAST",
        }
    }

    pub async fn write(&self, stream: &mut quinn::SendStream) -> Result<(), Error> {
        let message_bytes = bincode::serialize(self)?;

        let header = MessageHeader {
            version: MSG_PROTOCOL_VERSION,
            id: self.id(),
            reserved: [0u8; 2],
            len: message_bytes.len(),
        };

        let mut header_bytes = [0u8; HEADER_MESSAGE_LEN];
        header_bytes
            .write_with(&mut 0, header.clone(), byte::BE)
            .map_err(move |_e| Error::HeaderWrite(header_bytes.len() as u32, header))?;

        stream.write_all(&header_bytes).await?;
        stream.write_all(&message_bytes).await?;
        stream.flush().await.map_err(|e| Error::Other(e.into()))?;

        Ok(())
    }

    /// Read message from given stream
    ///
    /// Returns serialized `Message`, or `None` if stream has finished.
    pub async fn read(stream: &mut quinn::RecvStream) -> Result<Option<Self>, Error> {
        let mut header_bytes = [0u8; HEADER_MESSAGE_LEN];

        match stream.read_exact(&mut header_bytes).await {
            Err(quinn::ReadExactError::FinishedEarly) => {
                // stream is closed
                return Ok(None);
            }
            Err(e) => {
                return Err(Error::StreamRead(e));
            }
            Ok(_) => {}
        }

        let header: MessageHeader = header_bytes
            .read_with(&mut 0, byte::BE)
            .map_err(move |_e| Error::HeaderRead("Message header read failed".into()))?;

        let mut message_bytes = vec![0; header.len];
        stream.read_exact(&mut message_bytes).await?;

        let message: Message = bincode::deserialize(&message_bytes)?;

        Ok(Some(message))
    }
}

impl TryWrite<Endian> for MessageHeader {
    fn try_write(self, bytes: &mut [u8], ctx: Endian) -> byte::Result<usize> {
        use byte::*;

        check_len(bytes, HEADER_MESSAGE_LEN)?;

        if self.len > u32::MAX as usize {
            return Err(byte::Error::BadInput {
                err: "message payload is too big;",
            });
        }

        let offset = &mut 0;

        bytes.write_with::<u8>(offset, self.version, ctx)?;
        bytes.write_with::<u16>(offset, self.id as u16, ctx)?;
        bytes.write_with::<u32>(offset, self.len as u32, ctx)?;
        bytes.write_with::<u16>(offset, 0u16, ctx)?;

        Ok(*offset)
    }
}

impl TryRead<'_, Endian> for MessageHeader {
    fn try_read(bytes: &[u8], ctx: Endian) -> byte::Result<(Self, usize)> {
        use byte::*;

        check_len(bytes, HEADER_MESSAGE_LEN)?;

        let offset = &mut 0;

        let version = bytes.read_with::<u8>(offset, ctx)?;
        let id = bytes.read_with::<u16>(offset, ctx)?;
        let len = bytes.read_with::<u32>(offset, ctx)? as usize;

        let mut reserved = [0u8; 2];
        reserved[0] = bytes.read_with::<u8>(offset, ctx)?;
        reserved[1] = bytes.read_with::<u8>(offset, ctx)?;

        if version != MSG_PROTOCOL_VERSION {
            return Err(byte::Error::BadInput {
                err: "different protocol version;",
            });
        }

        let header = MessageHeader {
            version,
            id,
            len,
            reserved,
        };

        Ok((header, *offset))
    }
}
