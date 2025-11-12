use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use rand::Rng;
use tokio::sync::oneshot;

use crate::{fmt_id, raft::RaftResponseError};

// * Constants
const BASE_HEARTBEAT: f64 = 0.3;
pub(crate) const MAX_MSG_PER_APPEND_ENTRIES: u64 = 64;

#[derive(Debug)]
pub(crate) enum RaftMessage {
    RequestVote(proto::VoteRequest, oneshot::Sender<proto::VoteResponse>),
    AppendEntries(proto::AppendEntriesRequest, oneshot::Sender<proto::AppendEntriesResponse>),

    RequestVoteResponse(proto::VoteResponse),
    AppendEntriesResponse(proto::AppendEntriesResponse),

    HeartbeatTimeout(String),
    ElectionTimeout,

    SubmitWriteReq(proto::WriteOp, oneshot::Sender<Result<proto::WriteResp, RaftResponseError>>),
    SubmitReadReq(proto::ReadOp, oneshot::Sender<Result<proto::ReadResp, RaftResponseError>>),
}

impl Display for RaftMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::RequestVote(req, _) => {
                format!(
                    "[Vote REQ|{}:({})] term:{} last_log:({},{}) ",
                    fmt_id(&req.candidate_id),
                    req.req_id,
                    req.term,
                    req.last_log_term,
                    req.last_log_index
                )
            }

            Self::AppendEntries(req, _) => {
                format!(
                    "[Append entries REQ|{}:({}) -> {}] term:{} prev_log:({},{}) ldr_commit:{} entries:[{}..{}]({})",
                    fmt_id(&req.leader_id),
                    req.req_id,
                    fmt_id(&req.follower_id),
                    req.term,
                    req.prev_log_term,
                    req.prev_log_index,
                    req.leader_commit,
                    req.entries.first().map_or(0, |n| n.index),
                    req.entries.last().map_or(0, |n| n.index),
                    req.entries.len()
                )
            }

            Self::ElectionTimeout => "Election timeout".to_string(),
            Self::HeartbeatTimeout(id) => format!("Heartbeat timeout {}", fmt_id(&id)),

            Self::SubmitWriteReq(req, _) => format!("[Submit entry write-req] {:?}", req),
            Self::SubmitReadReq(req, _) => format!("[Submit entry read-req] {:?}", req),

            Self::AppendEntriesResponse(req) => {
                format!(
                    "[Append entries RESP|{}:({}) <- {}] term:{} success:{} entries:[{}..{}]({})",
                    fmt_id(&req.leader_id),
                    req.req_id,
                    fmt_id(&req.follower_id),
                    req.term,
                    req.success,
                    req.first_entry_index,
                    req.first_entry_index + req.entries_len,
                    req.entries_len
                )
            }
            Self::RequestVoteResponse(req) => {
                format!(
                    "[Vote RESP|{}:({})] term:{} granted:{}",
                    fmt_id(&req.from_id),
                    req.req_id,
                    req.term,
                    req.vote_granted
                )
            }
        };
        write!(f, "{}", name)
    }
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub(crate) enum Role {
    Leader,
    Candidate,
    Follower,
}
impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Role::Leader => "Ldr",
            Role::Candidate => "Cnd",
            Role::Follower => "Flw",
        };
        write!(f, "{}", name)
    }
}

#[inline]
fn randrange(min: f64, max: f64) -> f64 {
    rand::rng().random_range(min..=max)
}
#[inline]
pub(crate) fn heartbeat_dur() -> Duration {
    Duration::from_secs_f64(BASE_HEARTBEAT)
}
#[inline]
pub(crate) fn election_dur() -> Duration {
    Duration::from_secs_f64(BASE_HEARTBEAT * randrange(2.5, 5.0))
}
