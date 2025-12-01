use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{Mutex, MutexGuard, mpsc, oneshot};
use uuid::Uuid;

use crate::{
    fmt_id,
    raft::{
        RaftResponseError,
        log::Log,
        persist::Persist,
        state_machine::StateMachine,
        timer::Timer,
        transport::RaftPeer,
        types::{MAX_MSG_PER_APPEND_ENTRIES, RaftMessage, Role, election_dur, heartbeat_dur},
    },
};

/// RaftInner "owns itself" and can only be interacted with via sending RaftMessages via channels
pub(crate) struct RaftInner {
    msg_tx: mpsc::Sender<RaftMessage>,
    msg_rx: mpsc::Receiver<RaftMessage>,

    /// This holds two main things:
    /// 1. grpc connection object to make client requests to peer
    /// 2. match_index & next_index
    ///
    /// No need for this to be shared (arc etc) because it only will be changed by raft-msg handlers,
    /// which are processed serially.
    peers: HashMap<Uuid, Arc<Mutex<RaftPeer>>>,
    /// Our frontend uri
    client_uri: String,
    /// current leaders uri, to the best of our knowledge
    leader_api_uri: Option<String>, // uri
    leader_client_id: Option<Uuid>, // id

    election_timer: Option<Timer>,

    role: Role,
    // todo remove (just use [voted_for_by])
    votes_received: usize,
    /// Keeps track fo who voted for us, so duplicate messages dont make us count a vote from the same node
    /// twice
    voted_for_by: HashSet<uuid::Uuid>,

    persist: Persist,
    log: Arc<Log>,

    /// This has to be behind an arc, because the state-machine application is done in another task.
    state_machine: Arc<StateMachine>,
}

impl RaftInner {
    pub(crate) fn new(
        msg_tx: mpsc::Sender<RaftMessage>, msg_rx: mpsc::Receiver<RaftMessage>, persist: Persist,
        peers: HashMap<Uuid, Arc<Mutex<RaftPeer>>>, state_machine: Arc<StateMachine>, log: Arc<Log>,
        client_uri: String,
    ) -> Self {
        Self {
            log,
            peers,

            leader_api_uri: None,
            leader_client_id: None,

            client_uri,

            votes_received: 0,
            voted_for_by: HashSet::new(),
            persist,
            state_machine,

            election_timer: Some(Timer::new(election_dur(), {
                let tx = msg_tx.clone();
                async move || {
                    tx.send(RaftMessage::ElectionTimeout).await.unwrap();
                }
            })),
            role: Role::Follower,

            msg_tx,
            msg_rx,
        }
    }

    fn update_known_leader(&mut self, leader_uri_id: Option<(String, Uuid)>) {
        if let Some((new_uri, new_id)) = &leader_uri_id {
            if self.leader_client_id.as_ref().is_none_or(|old_id| old_id != new_id) {
                tracing::info!(
                    "New leader {} known with uri {} (term: {})",
                    fmt_id(&new_id),
                    &new_uri,
                    self.persist.local.term
                );
            }
        }

        if let Some((uri, id)) = leader_uri_id {
            self.leader_api_uri = Some(uri);
            self.leader_client_id = Some(id);
        } else {
            self.leader_api_uri = None;
            self.leader_client_id = None;
        }
    }

    async fn become_follower(&mut self, term: u64) -> Result<(), crate::Error> {
        for (_, peer) in self.peers.iter() {
            peer.lock().await.heartbeat_timer = None;
        }

        self.role = Role::Follower;

        self.state_machine.clear_op_reply_txs().await;
        self.persist.local.voted_for = None;
        self.persist.local.term = term;
        self.persist.save()?;

        self.new_election_timer();

        tracing::info!("Became FOLLOWER (term: {})", term);
        Ok(())
    }

    /// Can become leader instead if we are the only one in the cluster
    async fn become_candidate(&mut self) -> Result<(), crate::Error> {
        assert_ne!(self.role, Role::Leader, "Invalid transition, must be (Follower | Candidate) -> (Candidate)");

        for (_, peer) in self.peers.iter() {
            peer.lock().await.heartbeat_timer = None;
        }

        self.role = Role::Candidate;
        self.votes_received = 1;
        self.voted_for_by.clear();

        self.leader_api_uri = None;

        self.persist.local.term += 1;
        self.persist.local.voted_for = Some(self.persist.local.id.clone());
        self.persist.save()?;

        if self.peers.is_empty() {
            return self.become_leader().await;
        }

        self.new_election_timer();

        tracing::info!("Became CANDIDATE (term: {})", self.persist.local.term);
        Ok(())
    }

    async fn become_leader(&mut self) -> Result<(), crate::Error> {
        assert_eq!(self.role, Role::Candidate, "Invalid transition, must be: (Candidate) -> (Leader)");

        for (_, peer) in self.peers.iter() {
            peer.lock().await.heartbeat_timer = None;
        }

        let last_log_index = self.log.last_index()?;

        self.election_timer = None;
        self.role = Role::Leader;
        self.leader_api_uri = None;

        for (_, peer) in self.peers.iter() {
            let mut lock = peer.lock().await;
            lock.match_index = 0;
            lock.next_index = last_log_index + 1;
            lock.in_flight_ae_req = None;
            lock.heartbeat_timer = Some(self.new_heartbeat_timer(lock.id.clone()));
        }

        self.broadcast_elected_heartbeat().await?;

        tracing::info!("Became LEADER (term: {})", self.persist.local.term);
        Ok(())
    }

    /// Clears old timer and starts new election timer
    fn new_election_timer(&mut self) {
        assert_ne!(self.role, Role::Leader, "Leader shouldn't be starting an election timer");
        let tx = self.msg_tx.clone();
        self.election_timer = Some(Timer::new(election_dur(), async move || {
            tx.send(RaftMessage::ElectionTimeout).await.unwrap();
        }));
    }

    /// Clears old timer and starts new heartbeat timer
    fn new_heartbeat_timer(&self, id: Uuid) -> Timer {
        assert_eq!(self.role, Role::Leader, "Only leader should be starting a heartbeat timer");
        let tx = self.msg_tx.clone();
        Timer::new(heartbeat_dur(), async move || {
            tx.send(RaftMessage::HeartbeatTimeout(id)).await.unwrap();
        })
    }

    /// Only to be called as leader
    async fn submit_write_op_as_leader(
        &self, op: proto::WriteOp,
    ) -> Result<(u64, oneshot::Receiver<proto::WriteResp>), crate::Error> {
        assert_eq!(self.role, Role::Leader, "append to logs AS LEADER");
        let (tx, rx) = oneshot::channel();

        let entry = proto::LogEntry { op: Some(op), term: self.persist.local.term, index: self.log.last_index()? + 1 };
        let index = entry.index;

        self.log.insert(&entry)?;
        self.state_machine.add_op_reply_tx(index, tx).await;

        Ok((index, rx))
    }

    /// Should only be called as follower when receiving non-empty append-entries from leader.
    async fn add_logs_as_follower(&self, mut entries: Vec<proto::LogEntry>) -> Result<(), crate::Error> {
        assert_eq!(self.role, Role::Follower, "add to logs AS FOLLOWER");
        assert!(!entries.is_empty());
        // Careful here!
        //
        // 3.   If an existing entry conflicts with a new one (same index but different terms),
        //      delete the existing entry and all that follow it (§5.3)
        // 4.   Append any new entries not already in the log
        let mut truncated = false;
        for entry in entries.drain(..) {
            if !truncated && self.log.get(entry.index)?.map_or(false, |log| log.term != entry.term) {
                self.log.truncate(entry.index).await?;
                truncated = true; // so we dont keep rechecking
            }

            self.log.insert(&entry)?;
        }

        Ok(())
    }

    /// This is the ONLY way appendEntries is sent
    async fn send_append_entries(&self, mut peer: MutexGuard<'_, RaftPeer>) -> Result<(), crate::Error> {
        assert_eq!(self.role, Role::Leader, "Should only send append-entries as leader");
        let commit_index = self.state_machine.commit_index().await;

        let (prev_log_term, prev_log_index) = self.get_prev_log(&peer)?;

        let req: proto::AppendEntriesRequest = match &peer.in_flight_ae_req {
            Some(in_flight_req) => in_flight_req.clone(),

            None => {
                let entries = self.log.get_range(peer.next_index, peer.next_index + MAX_MSG_PER_APPEND_ENTRIES)?;

                let req = proto::AppendEntriesRequest {
                    req_id: peer.next_req_id(),
                    term: self.persist.local.term,
                    leader_id: self.persist.local.id.clone(),
                    follower_id: peer.id.into(),
                    prev_log_index,
                    prev_log_term,
                    leader_commit: commit_index,
                    entries: entries,
                    leader_api_uri: self.client_uri.clone(),
                };

                // todo make this less shit (just make an inflight-struct with (start..end) instead of entries)
                peer.in_flight_ae_req = Some(req.clone());
                peer.dirty = false;

                req
            }
        };

        peer.heartbeat_timer = Some(self.new_heartbeat_timer(peer.id.clone()));

        let tx = self.msg_tx.clone();
        let cloned_peer = peer.thin_clone();

        tokio::spawn(async move {
            cloned_peer.append_entries(req, tx).await;
        });

        Ok(())
    }

    async fn broadcast_elected_heartbeat(&self) -> Result<(), crate::Error> {
        self.submit_write_op_as_leader(proto::WriteOp { write_op: None }).await?;

        // commit index will be the same for all these - node is single threaded
        for (_, peer) in self.peers.iter() {
            let lock = peer.lock().await;
            self.send_append_entries(lock).await?;
        }
        Ok(())
    }

    /// Checks if we can commit more entries
    /// Should be called on receiving a successful reply to append-entries
    /// (as leader of course)
    async fn maybe_commit(&self) -> Result<(), crate::Error> {
        assert!(
            self.role == Role::Leader,
            "maybe_commit should only be called by leader (others simply increase index)"
        );
        let mut match_indicies: Vec<u64> = vec![];

        // todo make this not an edge case
        if self.peers.is_empty() {
            self.state_machine.set_commit_index(self.log.last_index()?).await;
            return Ok(());
        }

        for peer in self.peers.iter() {
            match_indicies.push(peer.1.lock().await.match_index);
        }

        for n in ((self.state_machine.commit_index().await + 1)..=self.log.last_index()?).rev() {
            let log = self.log.get(n)?;

            if let Some(log) = log {
                if log.term == self.persist.local.term {
                    let mut count = 0;

                    for &index in match_indicies.iter() {
                        if index >= n {
                            count += 1;
                        }
                        if count >= (match_indicies.len() / 2 + 1) {
                            self.state_machine.set_commit_index(n).await;
                            return Ok(());
                        }
                    }
                }
            } else {
                unreachable!("maybe_commit couldn't find entry - shouldn't happen")
            };
        }

        Ok(())
    }

    /// (prev_log_term, prev_log_index)
    fn get_prev_log(&self, peer: &RaftPeer) -> Result<(u64, u64), crate::Error> {
        if let Some(log) = self.log.get(peer.next_index - 1)? {
            Ok((log.term, log.index))
        } else {
            unreachable!(
                "No log entry at index ({}) found when trying to find prev_log for peer {}, shouldn't happen. 
                `peer.next_index` should be initialized to the latest entry in our (leader's) log, 
                so we should obviously have a log at (our_max_log_index - 1)",
                peer.next_index - 1,
                fmt_id(&peer.id)
            );
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), crate::Error> {
        tracing::info!("--- RAFT STARTING ---\n");

        while let Some(request_message) = self.msg_rx.recv().await {
            {
                let voted_for_id = match &self.persist.local.voted_for_uuid() {
                    Some(id) => fmt_id(id),
                    None => "[none]".into(),
                };
                tracing::trace!(
                    "{} (r-{}, t-{}, c-{}, l-{} v-{}) <- {}",
                    fmt_id(self.persist.local.uuid()),
                    self.role,
                    self.persist.local.term,
                    self.state_machine.commit_index().await,
                    self.log.last_index()?,
                    voted_for_id,
                    request_message
                );
            }

            match request_message {
                // we are being asked to append entries
                RaftMessage::AppendEntries(request, reply_tx) => {
                    tracing::trace!("{:#?}", &request);
                    // * if rpc req/resp has higher term than ours, convert to follower with newer term
                    if request.term > self.persist.local.term {
                        self.become_follower(request.term).await?;
                        self.update_known_leader(Some((request.leader_api_uri.clone(), request.leader_uuid().clone())));
                    }

                    // If we get a rpc from a "new leader" (which could have our same term) we step down
                    if let Role::Candidate = self.role
                        && request.term >= self.persist.local.term
                    {
                        self.become_follower(request.term).await?;
                        self.update_known_leader(Some((request.leader_api_uri.clone(), request.leader_uuid().clone())));
                    }

                    // now we can just handle the request if we are follower, otherwise ignore
                    if let Role::Follower = self.role {
                        let term = self.persist.local.term;

                        // * reply false if term < currentTerm
                        let resp = if term > request.term {
                            proto::AppendEntriesResponse::from((&request, term, false))
                        } else {
                            // Otherwise current leader - reset election timer
                            self.new_election_timer();
                            // this is our current leader, so we can set our leader_api_uri
                            self.update_known_leader(Some((
                                request.leader_api_uri.clone(),
                                request.leader_uuid().clone(),
                            )));

                            // * Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm
                            if !self
                                .log
                                .get(request.prev_log_index)?
                                .is_some_and(|value| value.term == request.prev_log_term)
                            {
                                proto::AppendEntriesResponse::from((&request, term, false))
                            } else if !request.entries.is_empty() {
                                let last_index_received = request.entries.last().unwrap().index;
                                let new_commit_index = request.leader_commit.min(last_index_received);
                                // construct here so we can move into add_logs_as_follower
                                let resp = proto::AppendEntriesResponse::from((&request, term, true));
                                self.add_logs_as_follower(request.entries).await?;
                                self.state_machine.set_commit_index(new_commit_index).await;
                                resp
                            } else {
                                self.state_machine
                                    .set_commit_index(request.leader_commit.min(request.prev_log_index))
                                    .await;
                                proto::AppendEntriesResponse::from((&request, term, true))
                            }
                        };

                        if let Err(_e) = reply_tx.send(resp) {
                            tracing::error!("Couldn't reply to append-entries");
                        }
                    }
                }

                RaftMessage::AppendEntriesResponse(response) => {
                    // * if rpc req/resp has higher term than ours, convert to follower with newer term
                    if response.term > self.persist.local.term {
                        // Can't garner leader from this without getting more complicated
                        // (We get these responses from non-leaders too)
                        self.become_follower(response.term).await?;
                        self.update_known_leader(None);

                        continue;
                    }

                    // to avoid processing late append-entries responses
                    if let Role::Leader = self.role {
                        let Some(peer_client) = self.peers.get(response.follower_uuid()) else {
                            unreachable!("Didn't have peer with id of responder??")
                        };

                        let mut peer_lock = peer_client.lock().await;

                        if peer_lock.highest_resp_req_id.is_some_and(|id| id >= response.req_id) {
                            // we've seen this, so this is just a duplicate response from a retry -> ignore
                            continue;
                        }

                        {
                            let Some(in_flight_req) = &peer_lock.in_flight_ae_req else {
                                unreachable!("Received resp with no inflight request")
                            };
                            assert_eq!(
                                in_flight_req.req_id, response.req_id,
                                "Non-stale response didn't match in-flight req id"
                            );
                        }

                        peer_lock.highest_resp_req_id = Some(response.req_id);
                        peer_lock.in_flight_ae_req = None;

                        if response.success {
                            if response.entries_len > 0 {
                                peer_lock.next_index = response.first_entry_index + response.entries_len;
                                peer_lock.match_index = peer_lock.next_index - 1;
                                drop(peer_lock);
                                self.maybe_commit().await?;
                                peer_lock = peer_client.lock().await;
                            }

                            // if we have new pending entries that we couldnt send at time of receipt
                            // because we already had a in-flight append-entries request for this peer,
                            // send a new append-entries right away
                            if peer_lock.dirty {
                                self.send_append_entries(peer_lock).await?;
                            }
                        } else {
                            // we enforce only 1 in-flight append-entries per peer at once,
                            // so this is sound.
                            peer_lock.next_index -= 1;

                            self.send_append_entries(peer_lock).await?;
                        }
                    }
                }

                // vote requested from us
                RaftMessage::RequestVote(request, reply_tx) => {
                    // * if rpc req/resp has higher term than ours, convert to follower with newer term
                    if request.term > self.persist.local.term {
                        self.become_follower(request.term).await?;
                        self.update_known_leader(None);
                    }

                    let req_id = request.req_id;

                    let resp = match self.role {
                        Role::Follower => {
                            let term = self.persist.local.term;
                            let from_id = self.persist.local.id.clone();

                            if request.term < self.persist.local.term {
                                proto::VoteResponse { req_id, term, vote_granted: false, from_id }
                            } else if !(self.persist.local.voted_for.clone())
                                .is_some_and(|id| id != request.candidate_id)
                                && self
                                    .log
                                    .candidates_log_up_to_date((request.last_log_term, request.last_log_index))?
                            {
                                // if we havent voted, or voted for them, and their logs are
                                // at least as up to date as ours
                                self.persist.local.voted_for = Some(request.candidate_id);
                                self.persist.save()?;

                                self.new_election_timer();
                                proto::VoteResponse { req_id, term: request.term, vote_granted: true, from_id }
                            } else {
                                proto::VoteResponse { req_id, term, vote_granted: false, from_id }
                            }
                        }
                        Role::Candidate | Role::Leader => {
                            // same or lower term, so we don't grant a vote (we already voted for ourselves)
                            proto::VoteResponse {
                                req_id,
                                term: self.persist.local.term,
                                vote_granted: false,
                                from_id: self.persist.local.id.clone(),
                            }
                        }
                    };

                    if let Err(_e) = reply_tx.send(resp) {
                        tracing::error!("Couldn't reply to request-vote");
                    }
                }

                RaftMessage::RequestVoteResponse(response) => {
                    // * if rpc req/resp has higher term than ours, convert to follower with newer term
                    if response.term > self.persist.local.term {
                        self.become_follower(response.term).await?;
                        self.update_known_leader(None);
                    }

                    if self.role == Role::Candidate {
                        // protect against old vote responses
                        if response.vote_granted && response.term == self.persist.local.term {
                            if !self.voted_for_by.contains(response.from_uuid()) {
                                self.votes_received += 1;
                                self.voted_for_by.insert(response.from_uuid().clone());
                                if self.votes_received as usize > (self.peers.len() + 1) / 2 {
                                    self.become_leader().await?;
                                }
                            }
                        }
                    }
                }

                RaftMessage::ElectionTimeout => {
                    if let Role::Follower | Role::Candidate = self.role {
                        // we are incrementing term every time
                        self.become_candidate().await?;
                        let (last_log_term, last_log_index) = self.log.last_log_term_index()?;

                        // not mutating so
                        for (_, peer_client) in self.peers.iter() {
                            // clone arc
                            let peer_client = peer_client.clone();

                            tokio::spawn({
                                let id = self.persist.local.id.clone();
                                let term = self.persist.local.term;
                                let tx = self.msg_tx.clone();
                                let leader_api_uri = self.client_uri.clone();

                                async move {
                                    let peer_lock = peer_client.lock().await;
                                    peer_lock
                                        .request_vote(
                                            proto::VoteRequest {
                                                req_id: peer_lock.next_req_id(),
                                                term,
                                                candidate_id: id,
                                                last_log_index,
                                                last_log_term,
                                                leader_api_uri,
                                            },
                                            tx,
                                        )
                                        .await;
                                }
                            });
                        }
                    } else {
                        // todo - this is a bit of a race condition where we can:
                        // 1. call becomeFollower fn
                        // 2. heartbeat timer pops
                        // 3. change to followerRole and reset timer
                        // now we are follower and have a heartbeat in our message bus (same for election&candidate&etc)
                        //unreachable!("Got heartbeat timeout as follower/candidate - shouldn't happen")
                        tracing::warn!(
                            "RACE CONDITION TODO: Got heartbeat timeout as follower/candidate - shouldn't happen. Discarding."
                        )
                        //unreachable!("Got election timeout but we were leader - shouldn't happen")
                    }
                }

                RaftMessage::HeartbeatTimeout(id) => {
                    if self.role == Role::Leader {
                        let peer =
                            self.peers.get(&id).expect(&format!("Peer not found for id? {}", fmt_id(&id))).lock().await;
                        self.send_append_entries(peer).await?;
                    } else {
                        // todo - this is a bit of a race condition where we can:
                        // 1. call becomeFollower fn
                        // 2. heartbeat timer pops
                        // 3. change to followerRole and reset timer
                        // now we are follower and have a heartbeat in our message bus (same for election&candidate&etc)
                        //unreachable!("Got heartbeat timeout as follower/candidate - shouldn't happen")
                        tracing::warn!(
                            "RACE CONDITION TODO: Got heartbeat timeout as follower/candidate - shouldn't happen. Discarding."
                        )
                    }
                }

                RaftMessage::SubmitReadReq(read_op, reply_tx) => match self.role {
                    Role::Leader => {
                        let _ = reply_tx.send(Ok(self.state_machine.read_op(read_op).await));
                    }
                    Role::Candidate | Role::Follower => {
                        let resp = match &self.leader_api_uri {
                            Some(uri) => Err(RaftResponseError::NotLeader { uri: uri.clone() }),
                            None => Err(RaftResponseError::NotReady),
                        };
                        let _ = reply_tx.send(resp);
                    }
                },

                RaftMessage::SubmitWriteReq(write_op, reply_tx) => {
                    match self.role {
                        Role::Leader => {
                            // log mutation
                            let (received_log_index, reply_rx) = self.submit_write_op_as_leader(write_op).await?;

                            for (_, peer_client) in self.peers.iter() {
                                // i think this check is imfallible
                                let mut peer_lock = peer_client.lock().await;

                                if received_log_index >= peer_lock.next_index {
                                    if peer_lock.in_flight_ae_req.is_none() {
                                        self.send_append_entries(peer_lock).await?;
                                    } else {
                                        peer_lock.dirty = true;
                                    }
                                }
                            }

                            if self.peers.is_empty() {
                                self.maybe_commit().await?;
                            }

                            tokio::spawn(async move {
                                let resp =
                                    if let Ok(rcv) = tokio::time::timeout(Duration::from_secs(10), reply_rx).await {
                                        match rcv {
                                            Err(e) => Err(RaftResponseError::Fail { msg: e.to_string() }),
                                            Ok(client_resp) => Ok(client_resp),
                                        }
                                    } else {
                                        Err(RaftResponseError::Timeout)
                                    };

                                let _ = reply_tx.send(resp);
                            });
                        }
                        Role::Candidate | Role::Follower => {
                            let resp = match &self.leader_api_uri {
                                Some(uri) => Err(RaftResponseError::NotLeader { uri: uri.clone() }),
                                None => Err(RaftResponseError::NotReady),
                            };
                            let _ = reply_tx.send(resp);
                        }
                    }
                }
            };
        }

        Ok(())
    }
}
