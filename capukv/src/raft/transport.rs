use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Result};

use crate::raft::timer::Timer;
use crate::raft::types::RaftMessage;
use crate::{fmt_id, raft};

const CONNECT_TIMEOUT_MS: u64 = 1500;

type RaftTonicClient = proto::raft_service_client::RaftServiceClient<tonic::transport::Channel>;
// todo refactor grpc+logging and raft state out from eachother here
pub(crate) struct RaftPeer {
    pub(crate) uri: String,
    pub(crate) id: uuid::Uuid,
    client: Arc<Mutex<Option<RaftTonicClient>>>,
    pub(crate) next_index: u64,
    pub(crate) match_index: u64,

    logging_meta: Arc<Mutex<LoggingMeta>>,

    // will refactor this a bit eventually - this is a req_id unique for term:peer_id
    // Resets every term and is not shared between peers
    req_id: Arc<AtomicU64>,
    // we have a new entry(s) to send to this guy, but we couldnt cause there was already
    // an in-flight request
    pub(crate) dirty: bool,
    /// This is actually "append entries in flight"
    pub(crate) in_flight_ae_req: Option<proto::AppendEntriesRequest>,
    /// This is the highest req_id we have gotten a response to
    pub(crate) highest_resp_req_id: Option<u64>,

    pub(crate) heartbeat_timer: Option<Timer>,
}

impl RaftPeer {
    #[must_use]
    pub(crate) fn new(uri: String, id: uuid::Uuid) -> Self {
        Self {
            uri,
            id,
            client: Arc::new(Mutex::new(None)),

            // these get overwritten on leader transition, so these values shouldn't matter
            match_index: 0,
            next_index: 1,

            req_id: Arc::new(AtomicU64::new(1)),
            logging_meta: Arc::new(Mutex::new(LoggingMeta {
                fails_since_log: 0,
                fails_since_success: 0,
                logs_since_success: 0,
                last_logged: None,
                last_success: None,
            })),

            highest_resp_req_id: None,
            in_flight_ae_req: None,
            dirty: false,

            heartbeat_timer: None,
        }
    }

    pub(crate) fn thin_clone(&self) -> Self {
        Self {
            uri: self.uri.clone(),
            id: self.id.clone(),
            client: self.client.clone(),
            next_index: self.next_index,
            match_index: self.match_index,
            logging_meta: self.logging_meta.clone(),
            req_id: self.req_id.clone(),
            dirty: self.dirty,
            in_flight_ae_req: None, // not used, don't bother cloning
            highest_resp_req_id: self.highest_resp_req_id,
            heartbeat_timer: None,
        }
    }

    #[inline]
    pub(crate) fn next_req_id(&self) -> u64 {
        self.req_id.fetch_add(1, std::sync::atomic::Ordering::Release)
    }

    async fn get_client(&self) -> Result<RaftTonicClient, crate::Error> {
        // initialize client if needed
        let mut lock = self.client.lock().await;
        let client = match &*lock {
            None => {
                let endpoint = Endpoint::from_shared(self.uri.clone())?;
                let client = tokio::time::timeout(Duration::from_millis(CONNECT_TIMEOUT_MS), async {
                    RaftTonicClient::connect(endpoint).await
                })
                .await
                .map_err(|_e| crate::Error::Transport("Timed-out getting connection".to_string()))??;

                *lock = Some(client.clone());
                client
            }
            Some(client) => client.clone(),
        };
        Ok(client)
    }

    const INITIAL_LOGS: usize = 3;
    const BACKOFFS: [Duration; 3] = [Duration::from_secs(5), Duration::from_secs(15), Duration::from_secs(60)];
    #[inline]
    async fn should_log_failure(&self) -> bool {
        let now = Instant::now();
        let mut lock = self.logging_meta.lock().await;

        lock.fails_since_success += 1;

        if lock.fails_since_success <= Self::INITIAL_LOGS {
            lock.fails_since_log = 0;
            lock.logs_since_success += 1;
            lock.last_logged = Some(now);
            return true;
        }

        let backoff_dur = Self::BACKOFFS[(Self::BACKOFFS.len() - 1).min(lock.logs_since_success - Self::INITIAL_LOGS)];

        if let Some(prev) = lock.last_logged {
            if now.duration_since(prev) > backoff_dur {
                lock.last_logged = Some(now);
                tracing::warn!(
                    "[peer-client {}] omitted {} error logs over last {} seconds",
                    fmt_id(&self.id),
                    lock.fails_since_log,
                    now.duration_since(prev).as_secs(),
                );
                if let Some(last_success) = lock.last_success {
                    tracing::warn!(
                        "[peer-client {}] {} errors since last success, which was {} seconds ago",
                        fmt_id(&self.id),
                        lock.fails_since_success,
                        now.duration_since(last_success).as_secs(),
                    );
                }
                lock.fails_since_log = 0;
                lock.logs_since_success += 1;
                true
            } else {
                lock.fails_since_log += 1;
                false
            }
        } else {
            unreachable!("We should have Some() previous time, because of INITIAL_LOGS > 0")
        }
    }

    pub(crate) async fn request_vote(&self, args: proto::VoteRequest, msg_tx: mpsc::Sender<RaftMessage>) {
        let err = match self.get_client().await {
            Ok(mut client) => {
                let mut req = tonic::Request::new(args);
                req.set_timeout(Duration::from_millis(1000));
                let result = client.request_vote(req).await;

                match result {
                    Ok(result) => {
                        msg_tx.send(RaftMessage::RequestVoteResponse(result.into_inner())).await.unwrap();
                        self.logging_meta.lock().await.success();
                        return;
                    }
                    Err(e) => e,
                }
            }
            Err(e) => e.into(),
        };
        *self.client.lock().await = None;
        if self.should_log_failure().await {
            tracing::warn!("[peer-client {}][grpc request_vote] failed -> {}", fmt_id(&self.id), err.to_string());
        }
    }

    pub(crate) async fn append_entries(&self, args: proto::AppendEntriesRequest, msg_tx: mpsc::Sender<RaftMessage>) {
        tracing::trace!(
            "SENDING APPEND-ENTRY <HEARTBEAT?> (req:{} -> {}) pli:({},{}) lc:{} {}",
            args.req_id,
            fmt_id(args.follower_uuid()),
            args.prev_log_term,
            args.prev_log_index,
            args.leader_commit,
            {
                match args.entries.len() {
                    0 => "".to_string(),
                    1 => format!("ent:[{}](1)", args.entries.first().unwrap().index),
                    len => format!(
                        "ent:[{}..{}]({})",
                        args.entries.first().unwrap().index,
                        args.entries.last().unwrap().index,
                        len
                    ),
                }
            }
        );

        let err = match self.get_client().await {
            Ok(mut client) => {
                let mut req = tonic::Request::new(args);
                req.set_timeout(Duration::from_millis(1000));
                let result = client.append_entries(req).await;

                match result {
                    Ok(result) => {
                        msg_tx.send(RaftMessage::AppendEntriesResponse(result.into_inner())).await.unwrap();
                        self.logging_meta.lock().await.success();
                        return;
                    }
                    Err(e) => e,
                }
            }
            Err(e) => e.into(),
        };
        *self.client.lock().await = None;
        if self.should_log_failure().await {
            tracing::warn!("[peer-client {}][grpc append_entries] failed -> {}", fmt_id(&self.id), err.to_string());
        }
    }
}

// todo timeouts
#[tonic::async_trait]
impl proto::raft_service_server::RaftService for raft::SharedRaft {
    async fn append_entries(
        &self, req: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>> {
        let (_, _, req) = req.into_parts();
        let receiver = self.submit_append_entries(req).await?;
        let resp = receiver.await.map_err(|e| tonic::Status::internal(format!("RecvError: {:?}", e)))?;
        Ok(resp.into())
    }

    async fn request_vote(&self, req: Request<proto::VoteRequest>) -> Result<Response<proto::VoteResponse>> {
        let (_, _, req) = req.into_parts();
        let receiver = self.submit_request_vote(req).await?;
        let resp = receiver.await.map_err(|e| tonic::Status::internal(format!("RecvError: {:?}", e)))?;
        Ok(resp.into())
    }
}
struct LoggingMeta {
    last_logged: Option<Instant>,
    last_success: Option<Instant>,
    fails_since_success: usize,
    fails_since_log: usize,
    logs_since_success: usize,
}

impl LoggingMeta {
    fn success(&mut self) {
        self.fails_since_log = 0; // a little wrong but
        self.fails_since_success = 0;
        self.logs_since_success = 0;
        self.last_logged = None;
        self.last_success = Some(Instant::now())
    }
}
