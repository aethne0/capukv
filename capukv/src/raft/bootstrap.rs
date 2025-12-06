/// This is a bit crude but is implemented to avoid having to manually pass IDs etc through config/cli
/// to start up nodes. To start performing Raft-stuff nodes need to know their peers, those peers uris,
/// ids, etc, and so we either have to pass them in manually or do some sort of bootstrapping or discovery
/// proccess.
///
/// In this bootstrapping process we simply (with an expected cluster size N+!):
///  1. start a small temporary gRPC server and wait for N dstinct peers to greet us
///  2. Contact (successfully) all our expected peers
/// These communicae will tell eachother IDs etc that have been generated so all the nodes
/// are "on the same page" to start off.
///
/// This is definitely not totally foolproof, but is acceptable for now as its just a one-time thing.
/// Can improve in the future (especially when cluster-membership-change support added to the raft section)
use std::{collections::HashSet, net::SocketAddr, time::Duration};

use tonic::{Request, Response, Result};

use crate::raft;
pub(crate) struct Bootstrapper {
    pub(crate) id: Vec<u8>,
    /// A uuid (bytes)
    pub(crate) heard_from: tokio::sync::mpsc::Sender<Vec<u8>>,
}

#[tonic::async_trait]
impl proto::init_service_server::InitService for Bootstrapper {
    async fn greet(&self, req: Request<proto::GreetRequest>) -> Result<Response<proto::GreetResponse>> {
        let (_, _, req) = req.into_parts();
        self.heard_from.send(req.id).await.expect("Error sending on bootstrapping channel");
        Ok(proto::GreetResponse { id: self.id.clone() }.into())
    }
}

pub(crate) async fn bootstrap(
    mut peer_uris: Vec<tonic::transport::Uri>, raft_addr: SocketAddr, persist: &mut raft::Persist,
) -> Result<(), crate::Error> {
    tracing::info!("Starting bootstrapping server on {}...", raft_addr);
    let expected = peer_uris.len();
    let id = persist.local.id.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel(expected);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (peers_tx, mut peers_rx) = tokio::sync::mpsc::channel(expected);
    let mut heard_from = HashSet::new();

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::init_service_server::InitServiceServer::new(Bootstrapper { id, heard_from: tx }))
            .serve_with_shutdown(raft_addr, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect(&format!("Couldn't start cluster bootstrapping gRPC server on {} (raft-addr)", raft_addr))
    });

    for uri_outer in peer_uris.drain(..) {
        tokio::spawn({
            let id = persist.local.id.clone();
            let peers_tx = peers_tx.clone();
            async move {
                loop {
                    let uri = uri_outer.clone();
                    let id = id.clone();

                    match {
                        async {
                            let res = tokio::time::timeout(Duration::from_secs(2), async move {
                                proto::init_service_client::InitServiceClient::connect(uri.clone()).await
                            })
                            .await
                            .map_err(|e| e.to_string())?;
                            let mut client = res.map_err(|e| e.to_string())?;
                            let res = client.greet(proto::GreetRequest { id }).await.map_err(|e| e.to_string())?;
                            Ok::<Vec<u8>, String>(res.into_inner().id)
                        }
                        .await
                    } {
                        Ok(id) => {
                            let _ = peers_tx.send((uri_outer, id)).await;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!("Couldn't contact peer ({}) while bootstrapping: {}", &uri_outer, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        });
    }

    let mut peers = vec![];
    while let Some((uri, id)) = peers_rx.recv().await {
        tracing::info!("Contacted peer at {}", &uri);
        peers.push(proto::Peer { uri: uri.to_string(), id });
        if peers.len() == expected {
            break;
        }
    }

    persist.local.peers = peers;
    persist.save()?;

    while let Some(rcvd) = rx.recv().await {
        tracing::info!("Heard from peer {}", uuid::Uuid::from_slice(&rcvd).unwrap());
        heard_from.insert(rcvd);
        if heard_from.len() == expected {
            // everyone has pinged us, so we can close server
            let _ = shutdown_tx.send(());
            break;
        }
    }

    let _ = server_handle.await; // wait for server to close, because we need the port back:W

    Ok(())
}
