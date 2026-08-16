use crate::backend::GenericSocketBackend;
use crate::codec::{Message, ZmqFramedRead};
use crate::fair_queue::FairQueue;
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{
    Endpoint, MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketOptions, SocketRecv,
    SocketSend, SocketType, ZmqError, ZmqMessage, ZmqResult,
};

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::StreamExt;

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

/// Exclusive pair socket (`ZMQ_PAIR`).
///
/// A `PAIR` socket is connected to exactly one peer. Further connection attempts
/// are rejected (the extra peer is dropped), matching libzmq behaviour.
pub struct PairSocket {
    backend: Arc<GenericSocketBackend>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for PairSocket {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

#[async_trait]
impl Socket for PairSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(GenericSocketBackend::with_options(
            Some(fair_queue.inner()),
            SocketType::PAIR,
            options,
        ));

        let backend_weak = Arc::downgrade(&backend);
        fair_queue.set_on_disconnect(move |peer_id: PeerIdentity| {
            if let Some(backend) = backend_weak.upgrade() {
                backend.peer_disconnected(&peer_id);
            }
        });

        Self {
            backend,
            fair_queue,
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle, RandomState> {
        &mut self.binds
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[async_trait]
impl SocketRecv for PairSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((_peer_id, Ok(Message::Message(message)))) => {
                    return Ok(message);
                }
                Some((_peer_id, Ok(_))) => {
                    // Ignore non-message frames
                }
                Some((peer_id, Err(e))) => {
                    self.backend.peer_disconnected(&peer_id);
                    return Err(e.into());
                }
                None => {
                    return Err(ZmqError::NoMessage);
                }
            }
        }
    }
}

#[async_trait]
impl SocketSend for PairSocket {
    async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
        self.backend
            .send_round_robin(Message::Message(message))
            .await?;
        Ok(())
    }
}
