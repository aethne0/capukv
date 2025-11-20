mod api;
mod err;
pub mod logging;
mod raft;

pub use err::Error;

use crate::raft::Raft;

pub struct ArgPeer {
    pub uri: String,
    pub id: uuid::Uuid,
}

pub(crate) fn fmt_id(uuid: &uuid::Uuid) -> String {
    format!("{{{}}}", uuid.as_hyphenated())
}

pub struct CapuKv {}

impl CapuKv {
    #[must_use]
    pub async fn build_and_run(
        id: uuid::Uuid, path: &std::path::Path, addr: std::net::SocketAddr, peers: Vec<ArgPeer>, frontend_uri: String,
    ) -> Result<(), crate::Error> {
        // todo
        let frontend_addr = {
            use std::str::FromStr;
            std::net::SocketAddr::from_str(&frontend_uri).unwrap()
        };
        let raft = std::sync::Arc::new(Raft::new(id, path, peers, frontend_uri).await?);

        let raft_handle = tokio::spawn({
            let raft: crate::raft::SharedRaft = raft.clone().into();
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::raft_service_server::RaftServiceServer::new(raft))
                    .serve(addr)
                    .await
                    .unwrap();
            }
        });

        let api_handle = tokio::spawn({
            let raft: crate::raft::SharedRaft = raft.clone().into();
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::api_service_server::ApiServiceServer::new(raft))
                    .serve(frontend_addr)
                    .await
                    .unwrap();
            }
        });

        tokio::select! {
            _ = raft_handle => {
                tracing::error!("Raft server task joined");
            }
            _ = api_handle => {
                tracing::error!("Api server task joined");
            }
        }

        Ok(())
    }
}
