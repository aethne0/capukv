use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::{Mutex, mpsc, oneshot};

mod bootstrap;
mod db_open;
mod log;
mod node;
mod persist;
mod state_machine;
mod timer;
mod transport;
mod types;

use crate::{
    err::RaftResponseError,
    fmt_id,
    raft::{
        bootstrap::bootstrap, db_open::open_db, log::Log, node::RaftInner, persist::Persist,
        state_machine::StateMachine, transport::RaftPeer, types::RaftMessage,
    },
};

pub(crate) struct Raft {
    // message queue
    msg_tx: mpsc::Sender<RaftMessage>,
}

impl Raft {
    #[must_use]
    pub(crate) async fn new(
        dir: &std::path::Path, raft_addr: SocketAddr, peer_uris: Vec<tonic::transport::Uri>,
        redirect_uri: Option<tonic::transport::Uri>,
    ) -> Result<Self, crate::Error> {
        let db = open_db(dir);
        let mut persist = Persist::new(db.clone())?;
        let log = Arc::new(Log::new(db.clone())?);

        // bootstrapping, if needed
        // strictly speaking this is not bulletproof, but we can improve later by making it commited into the log etc
        // for now i just wanted to reduce the complexity of the config (ie not specifying all the ids etc manually)
        if persist.local.peers.is_empty() {
            bootstrap(peer_uris, raft_addr, &mut persist).await?;
        }

        let state_machine = Arc::new(StateMachine::new(log.clone()));

        tracing::info!("Our ID: {}", fmt_id(&persist.local.uuid()));
        tracing::info!(
            "{}",
            format!(
                "Starting with: id: {}, term: {}, voted_for: {}",
                persist.local.uuid().as_hyphenated(),
                persist.local.term,
                persist.local.voted_for_uuid().map_or("None".to_string(), |u| u.as_hyphenated().to_string())
            )
        );

        let last_log = log.last_index().unwrap();
        if last_log > 0 {
            tracing::info!("Initializing state machine with {} persisted logs...", last_log);
        }

        let (msg_tx, msg_rx) = mpsc::channel(64); // todo i really didnt think about this number

        let mut peers_map = HashMap::new();
        for (uri, uuid) in persist.local.peers() {
            if uuid != *persist.local.uuid() {
                let peer = RaftPeer::new(uri, uuid);
                peers_map.insert(uuid, Arc::new(Mutex::new(peer)));
            }
        }

        let mut inner = RaftInner::new(msg_tx.clone(), msg_rx, persist, peers_map, state_machine, log, redirect_uri);

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
