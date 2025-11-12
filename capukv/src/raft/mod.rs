use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, mpsc, oneshot};

mod db;
mod log;
mod node;
mod persist;
mod state_machine;
mod timer;
mod transport;
mod types;

use crate::{
    ArgPeer,
    err::RaftResponseError,
    fmt_id,
    raft::{
        db::get_db, log::Log, node::RaftInner, persist::Persist, state_machine::StateMachine, transport::RaftPeer,
        types::RaftMessage,
    },
};

pub(crate) struct SharedRaft(Arc<Raft>);
impl std::ops::Deref for SharedRaft {
    type Target = Arc<Raft>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<Arc<Raft>> for SharedRaft {
    fn from(value: Arc<Raft>) -> Self {
        Self(value)
    }
}

pub(crate) struct Raft {
    // message queue
    msg_tx: mpsc::Sender<RaftMessage>,
}

impl Raft {
    #[must_use]
    pub(crate) async fn new(
        id: uuid::Uuid, path: &std::path::Path, mut peers: Vec<ArgPeer>, frontend_uri: String,
    ) -> Result<Self, crate::Error> {
        let db = get_db(path);
        let persist = Persist::new(id, db.clone())?;
        let log = Arc::new(Log::new(db.clone())?);

        let state_machine = Arc::new(StateMachine::new(log.clone()));

        tracing::info!("Our ID: {}", fmt_id(&persist.local.uuid()));
        tracing::info!("Starting with: {:?}", persist.local);
        if peers.len() > 0 {
            tracing::info!("Peers:",);
            for p in peers.iter() {
                tracing::info!("|--- Peer {} & {}", fmt_id(&p.id), &p.uri);
            }
        } else {
            tracing::info!("0 peers")
        }

        let last_log = log.last_index().unwrap();
        if last_log > 0 {
            tracing::info!("Initializing state machine with {} persisted logs...", last_log);
        }

        let (msg_tx, msg_rx) = mpsc::channel(1024);

        let mut peers_map = HashMap::new();
        for p in peers.drain(..) {
            let peer = RaftPeer::new(p.uri, p.id.clone());
            peers_map.insert(p.id, Arc::new(Mutex::new(peer)));
        }

        let mut inner = RaftInner::new(msg_tx.clone(), msg_rx, persist, peers_map, state_machine, log, frontend_uri);

        tokio::spawn(async move { inner.run().await });

        Ok(Self { msg_tx: msg_tx })
    }

    /// For transport layer to submit request-vote messages into the node
    pub(crate) async fn submit_request_vote(
        &self, req: proto::VoteRequest,
    ) -> Result<oneshot::Receiver<proto::VoteResponse>, crate::Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self
            .msg_tx
            .send(RaftMessage::RequestVote(req, tx))
            .await
            .map_err(|e| crate::Error::Raft(format!("Raft core [request-vote] error: {}", e.to_string())))
        {
            panic!("Raft msg-channel closed | {:?}", e);
        }
        Ok(rx)
    }

    /// For transport layer to submit append-entries messages into the node
    pub(crate) async fn submit_append_entries(
        &self, req: proto::AppendEntriesRequest,
    ) -> Result<oneshot::Receiver<proto::AppendEntriesResponse>, crate::Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self
            .msg_tx
            .send(RaftMessage::AppendEntries(req, tx))
            .await
            .map_err(|e| crate::Error::Raft(format!("Raft core [append-entries] error: {}", e.to_string())))
        {
            panic!("Raft msg-channel closed | {:?}", e);
        }
        Ok(rx)
    }

    /// Client write
    pub(crate) async fn submit_write_op(
        &self, write_op: proto::WriteOp,
    ) -> Result<proto::WriteResp, RaftResponseError> {
        tracing::debug!("WriteOp: {:?}", &write_op);
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self
            .msg_tx
            .send(RaftMessage::SubmitWriteReq(write_op, tx))
            .await
            .map_err(|e| crate::Error::Raft(format!("Raft core [submit-write] error: {}", e.to_string())))
        {
            panic!("Raft msg-channel closed | {:?}", e);
        }

        let rx_res = rx.await.map_err(|_e| RaftResponseError::OperationCancelled);
        rx_res?
    }

    /// Client linear read
    pub(crate) async fn submit_read_op(&self, read_op: proto::ReadOp) -> Result<proto::ReadResp, RaftResponseError> {
        tracing::debug!("ReadOp: {:?}", &read_op);
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self
            .msg_tx
            .send(RaftMessage::SubmitReadReq(read_op, tx))
            .await
            .map_err(|e| crate::Error::Raft(format!("Raft core [submit-read] error: {}", e.to_string())))
        {
            panic!("Raft msg-channel closed | {:?}", e);
        }

        let rx_res = rx.await.map_err(|_e| RaftResponseError::OperationCancelled);
        rx_res?
    }
}
