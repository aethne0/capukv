#[derive(Debug, Clone)]
pub enum RaftResponseError {
    /// This is a catchall failure for "you cant do anything about this its just internally cooked"
    Fail {
        msg: String,
    },
    Timeout,
    NotReady,
    NotLeader {
        uri: String,
    },
    /// There was some state change and the reply channel got purged -
    /// for example node became follower before it could handle req
    ///
    /// Note: This may not mean the operation didn't happen (i have to check still)
    OperationCancelled,
}

#[derive(Debug)]
pub enum Error {
    Persistence(String),
    Codec(String),
    Transport(String),
    Rpc(String),
    Raft(String),
    Other(String),
}

impl From<tokio::task::JoinError> for crate::Error {
    fn from(value: tokio::task::JoinError) -> Self {
        crate::Error::Other(value.to_string())
    }
}

impl From<rust_rocksdb::Error> for crate::Error {
    fn from(value: rust_rocksdb::Error) -> Self {
        crate::Error::Persistence(value.to_string())
    }
}

impl From<prost::DecodeError> for crate::Error {
    fn from(value: prost::DecodeError) -> Self {
        crate::Error::Codec(value.to_string())
    }
}
impl From<prost::EncodeError> for crate::Error {
    fn from(value: prost::EncodeError) -> Self {
        crate::Error::Codec(value.to_string())
    }
}

impl From<tonic::transport::Error> for crate::Error {
    fn from(value: tonic::transport::Error) -> Self {
        crate::Error::Transport(value.to_string())
    }
}
impl From<tonic::ConnectError> for crate::Error {
    fn from(value: tonic::ConnectError) -> Self {
        crate::Error::Transport(value.to_string())
    }
}
impl From<tonic::Status> for crate::Error {
    fn from(value: tonic::Status) -> Self {
        crate::Error::Rpc(value.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for crate::Error {
    fn from(value: tokio::time::error::Elapsed) -> Self {
        crate::Error::Transport(value.to_string())
    }
}

impl From<crate::Error> for tonic::Status {
    fn from(value: crate::Error) -> Self {
        match value {
            Error::Transport(msg) => tonic::Status::unavailable(msg),
            Error::Codec(msg) => tonic::Status::internal(msg),
            Error::Persistence(msg) => tonic::Status::internal(msg),
            Error::Raft(msg) => tonic::Status::internal(msg),
            Error::Rpc(msg) => tonic::Status::internal(msg), // this one might need more detail
            Error::Other(msg) => tonic::Status::internal(msg), // this one might need more detail
        }
    }
}
