mod api;
mod err;
mod raft;

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{OnceLock},
};

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

static RAFT_INSTANCE: OnceLock<Raft> = OnceLock::new();

impl CapuKv {
    #[must_use]
    pub async fn build_and_run(
        dir: PathBuf,
        raft_addr: SocketAddr,
        api_addr: SocketAddr,
        peer_uris: Vec<tonic::transport::Uri>,
        redirect_uri: Option<tonic::transport::Uri>,
    ) -> Result<(), crate::Error> {
        let _ = RAFT_INSTANCE.set(Raft::new(&dir, raft_addr, peer_uris, redirect_uri).await?);

        let raft_handle = tokio::spawn({
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::raft_service_server::RaftServiceServer::new(
                        RAFT_INSTANCE.get().unwrap(),
                    ))
                    .serve(raft_addr)
                    .await
                    .unwrap();
            }
        });

        let api_handle = tokio::spawn({
            async move {
                tonic::transport::Server::builder()
                    .add_service(proto::api_service_server::ApiServiceServer::new(
                        RAFT_INSTANCE.get().unwrap(),
                    ))
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
