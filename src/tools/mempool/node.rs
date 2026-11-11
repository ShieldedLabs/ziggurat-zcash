//! The P2P side: handshakes with `relay: true`, answers peer requests, and forwards
//! inv, tx and notfound messages to the core.

use std::{io, net::SocketAddr};

use futures_util::SinkExt;
use pea2pea::{
    protocols::{Disconnect, Handshake, Reading, Writing},
    Config, Connection, ConnectionSide, Node, Pea2Pea,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::Framed;
use ziggurat_zcash::{
    protocol::{
        message::{constants::NU7_PROTOCOL_VERSION, Message},
        payload::{block::Headers, Addr, VarStr, Version},
    },
    tools::synthetic_node::MessageCodec,
};

pub enum NodeEvent {
    /// The peer sent its Version.
    Version(SocketAddr, String),
    /// The peer sent Verack, so it accepts requests now.
    Ready(SocketAddr),
    Message(SocketAddr, Message),
    ConnectFailed(SocketAddr, String),
    Disconnected(SocketAddr),
}

#[derive(Clone)]
pub struct Listener {
    node: Node,
    user_agent: String,
    events: UnboundedSender<NodeEvent>,
}

impl Pea2Pea for Listener {
    fn node(&self) -> &Node {
        &self.node
    }
}

impl Listener {
    pub fn new(
        user_agent: String,
        max_connections: u16,
        events: UnboundedSender<NodeEvent>,
    ) -> Self {
        let config = Config {
            name: Some("mempool".into()),
            listener_ip: None,
            max_connections,
            ..Default::default()
        };
        Self {
            node: Node::new(config),
            user_agent,
            events,
        }
    }

    pub async fn enable(&self) {
        self.enable_handshake().await;
        self.enable_reading().await;
        self.enable_writing().await;
        self.enable_disconnect().await;
    }

    /// Dials a peer in the background. A failure comes back as `ConnectFailed`.
    pub fn dial(&self, addr: SocketAddr) {
        let listener = self.clone();
        tokio::spawn(async move {
            if let Err(e) = listener.node.connect(addr).await {
                let _ = listener
                    .events
                    .send(NodeEvent::ConnectFailed(addr, e.to_string()));
            }
        });
    }

    /// Queues a message without waiting for the write.
    pub fn send(&self, addr: SocketAddr, message: Message) {
        let _ = self.unicast(addr, message);
    }

    pub fn is_up(&self, addr: SocketAddr) -> bool {
        self.node.is_connected(addr) || self.node.is_connecting(addr)
    }
}

#[async_trait::async_trait]
impl Handshake for Listener {
    const TIMEOUT_MS: u64 = 10_000;

    async fn perform_handshake(&self, mut conn: Connection) -> io::Result<Connection> {
        let addr = conn.addr();
        let mut framed = Framed::new(self.borrow_stream(&mut conn), MessageCodec::default());

        let mut version =
            Version::new(addr, ([127, 0, 0, 1], 0).into()).with_version(NU7_PROTOCOL_VERSION);
        version.user_agent = VarStr(self.user_agent.clone());
        version.relay = true;
        framed.send(Message::Version(version)).await?;

        // Zebra can delay its Version, so process_message handles it.
        Ok(conn)
    }
}

#[async_trait::async_trait]
impl Reading for Listener {
    type Message = Message;
    type Codec = MessageCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    async fn process_message(&self, source: SocketAddr, message: Message) -> io::Result<()> {
        match message {
            Message::Version(version) => {
                self.send(source, Message::Verack);
                let _ = self
                    .events
                    .send(NodeEvent::Version(source, version.user_agent.0));
            }
            Message::Verack => {
                let _ = self.events.send(NodeEvent::Ready(source));
            }
            Message::Ping(nonce) => self.send(source, Message::Pong(nonce)),
            Message::GetAddr => self.send(source, Message::Addr(Addr::empty())),
            Message::GetHeaders(_) => self.send(source, Message::Headers(Headers::empty())),
            Message::GetData(inv) => self.send(source, Message::NotFound(inv)),
            Message::Inv(_) | Message::Tx(_) | Message::NotFound(_) => {
                let _ = self.events.send(NodeEvent::Message(source, message));
            }
            _ => {}
        }
        Ok(())
    }
}

impl Writing for Listener {
    type Message = Message;
    type Codec = MessageCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

#[async_trait::async_trait]
impl Disconnect for Listener {
    async fn handle_disconnect(&self, addr: SocketAddr) {
        let _ = self.events.send(NodeEvent::Disconnected(addr));
    }
}
