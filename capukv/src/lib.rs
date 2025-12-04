mod api;
mod err;
mod raft;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

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
        dir: PathBuf, raft_addr: SocketAddr, api_addr: SocketAddr, peer_uris: Vec<tonic::transport::Uri>,
    ) -> Result<(), crate::Error> {
        let raft_instance = Arc::new(Raft::new(&dir, raft_addr, api_addr, peer_uris).await?);

        let raft_handle = tokio::spawn({
            let raft: crate::raft::SharedRaft = raft_instance.clone().into();
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::raft_service_server::RaftServiceServer::new(raft))
                    .serve(raft_addr)
                    .await
                    .unwrap();
            }
        });

        let api_handle = tokio::spawn({
            let raft: crate::raft::SharedRaft = raft_instance.clone().into();
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::api_service_server::ApiServiceServer::new(raft))
                    .serve(api_addr)
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
