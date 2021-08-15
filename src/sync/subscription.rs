use crate::reliable_conn::ReliableOrderedConnectionToTarget;
use crate::multiplex::{MultiplexedConnKey, MultiplexedPacket, MultiplexedConn};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use parking_lot::RwLock;
use std::collections::HashMap;
use crate::sync::network_endpoint::{PostActionChannel, PreActionChannel, PreActionSync, PostActionSync};
use crate::sync::RelativeNodeType;
use bytes::Bytes;
use std::net::SocketAddr;
use async_trait::async_trait;

#[async_trait]
pub trait SubscriptionBiStream: Send + Sync {
    type Conn: ReliableOrderedConnectionToTarget + 'static;
    type ID: MultiplexedConnKey;

    fn conn(&self) -> &Self::Conn;
    fn receiver(&self) -> &Mutex<UnboundedReceiver<Vec<u8>>>;
    fn id(&self) -> Self::ID;
    fn node_type(&self) -> RelativeNodeType;

    /// Creates a new multiplexed level capable of obtaining more subscribers.
    /// Uses Self as a reliable ordered connection, while using NewId to identify the substreams in the created next level
    async fn multiplex<NewID: MultiplexedConnKey + 'static>(self) -> Result<MultiplexedConn<Self, NewID>, anyhow::Error>
        where Self: Sized + 'static {
        MultiplexedConn::register(self.node_type(), self).await
    }
}

#[async_trait::async_trait]
pub trait Subscribable: Send + Sync + Sized {
    type ID: MultiplexedConnKey;
    type UnderlyingConn: ReliableOrderedConnectionToTarget + 'static;
    type SubscriptionType: SubscriptionBiStream;
    type BorrowedSubscriptionType<'a>: SubscriptionBiStream<ID=Self::ID, Conn=Self::UnderlyingConn> + Into<Self::SubscriptionType>;

    fn underlying_conn(&self) -> &Self::UnderlyingConn;
    fn subscriptions(&self) -> &RwLock<HashMap<Self::ID, UnboundedSender<Vec<u8>>>>;
    fn post_close_container(&self) -> &PostActionChannel<Self::ID>;
    fn pre_action_container(&self) -> &PreActionChannel<Self::ID>;

    async fn recv_post_close_signal_from_stream(&self, id: Self::ID) -> Result<(), anyhow::Error>;
    async fn send_post_close_signal(&self, id: Self::ID) -> Result<(), anyhow::Error>;
    async fn send_pre_open_signal(&self, id: Self::ID) -> Result<(), anyhow::Error>;

    fn node_type(&self) -> RelativeNodeType;

    fn initiate_subscription(&self) -> PreActionSync<'_, Self, Self::UnderlyingConn> {
        PreActionSync::new(self)
    }

    fn subscribe(&self, id: Self::ID) -> Self::BorrowedSubscriptionType<'_>;
    fn owned_subscription(&self, id: Self::ID) -> Self::SubscriptionType;
    fn get_next_id(&self) -> Self::ID;
}

#[async_trait]
impl<R: SubscriptionBiStream> ReliableOrderedConnectionToTarget for R {
    async fn send_to_peer(&self, input: &[u8]) -> std::io::Result<()> {
        let packet = MultiplexedPacket::ApplicationLayer { id: self.id(), payload: input.to_vec() };
        self.conn().send_to_peer(&bincode2::serialize(&packet).unwrap()).await
    }

    async fn recv(&self) -> std::io::Result<Bytes> {
        self.receiver().lock().await.recv().await.map(Bytes::from).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::ConnectionReset, "Receiver died"))
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.conn().local_addr()
    }

    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.conn().peer_addr()
    }
}

pub(crate) fn close_sequence_for_multiplexed_bistream<S: Subscribable<ID=K> + 'static, K: MultiplexedConnKey + 'static>(id: K, ptr: S) {
    log::info!("Running DROP on {:?}", id);

    fn close<S: Subscribable<ID=K>, K: MultiplexedConnKey>(id: K, ptr: &S) {
        let _ = ptr.subscriptions().write().remove(&id);
        log::info!("DROPPED id = {:?}", id);
    }

    // the runtime may not exist while dropping
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        rt.spawn(async move {
            if let Err(err) = PostActionSync::new(&ptr, id).await {
                log::warn!("[MetaActionSync/close] error: {:?}", err.to_string())
            }

            close(id, &ptr)
        });
    } else {
        close(id, &ptr);
    }
}