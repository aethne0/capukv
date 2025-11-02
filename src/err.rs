#[derive(Debug, Clone)]
pub enum RaftResponseError {
    Fail { msg: String },
    Timeout,
    NotReady,
    NotLeader { uri: String },
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

impl From<fjall::Error> for crate::Error {
    fn from(value: fjall::Error) -> Self {
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
