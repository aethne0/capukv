use std::sync::Arc;

use crate::{proto, raft::Raft};

async fn recv_write(
    rx: impl Future<Output = tokio::sync::oneshot::Receiver<Result<proto::WriteResp, crate::err::RaftResponseError>>>,
) -> Result<proto::write_ok::WriteOk, tonic::Status> {
    let resp = rx.await.await;
    tracing::debug!("WriteOp Resp: {:?}", &resp);
    match resp {
        Ok(val) => match val {
            Err(e) => match e {
                crate::err::RaftResponseError::Fail { msg } => Err(tonic::Status::internal(format!("{}", msg))),
                // todo leader messaging
                crate::err::RaftResponseError::NotLeader { uri } => {
                    Err(tonic::Status::failed_precondition(format!("{}", uri)))
                }
                crate::err::RaftResponseError::NotReady => Err(tonic::Status::unavailable("".to_string())),
                // todo probably wrong code for this
                crate::err::RaftResponseError::Timeout => {
                    Err(tonic::Status::deadline_exceeded("Timed out".to_string()))
                }
            },
            Ok(val) => match val.write_resp.unwrap() {
                proto::write_resp::WriteResp::Ok(val) => Ok(val.write_ok.unwrap()),
                proto::write_resp::WriteResp::Err(err) => match proto::Err::try_from(err).unwrap() {
                    proto::Err::KeyspaceNotFound => Err(tonic::Status::not_found("Keyspace".to_string())),
                    proto::Err::SnapshotNotFound => Err(tonic::Status::not_found("Snapshot".to_string())),
                    proto::Err::CasFailure => Err(tonic::Status::failed_precondition(
                        "CAS basis failure - one or more basis keys were mutated since snapshot creation",
                    )),
                },
            },
        },
        Err(e) => Err(tonic::Status::internal(format!("Reply_tx closed | {}", e.to_string()))),
    }
}

async fn recv_read(
    rx: impl Future<Output = tokio::sync::oneshot::Receiver<Result<proto::ReadResp, crate::err::RaftResponseError>>>,
) -> Result<proto::read_ok::ReadOk, tonic::Status> {
    let resp = rx.await.await;
    tracing::debug!("ReadOp Resp: {:?}", &resp);
    match resp {
        Ok(val) => match val {
            Err(e) => match e {
                crate::err::RaftResponseError::Fail { msg } => Err(tonic::Status::internal(format!("{}", msg))),
                // todo leader messaging
                crate::err::RaftResponseError::NotLeader { uri } => {
                    Err(tonic::Status::failed_precondition(format!("{}", uri)))
                }
                crate::err::RaftResponseError::NotReady => Err(tonic::Status::unavailable("".to_string())),
                // todo probably wrong code for this
                crate::err::RaftResponseError::Timeout => {
                    Err(tonic::Status::deadline_exceeded("Timed out".to_string()))
                }
            },
            Ok(val) => match val.read_resp.unwrap() {
                proto::read_resp::ReadResp::Ok(val) => Ok(val.read_ok.unwrap()),
                proto::read_resp::ReadResp::Err(err) => match proto::Err::try_from(err).unwrap() {
                    proto::Err::KeyspaceNotFound => Err(tonic::Status::not_found("Keyspace".to_string())),
                    proto::Err::SnapshotNotFound => Err(tonic::Status::not_found("Snapshot".to_string())),
                    proto::Err::CasFailure => Err(tonic::Status::failed_precondition(
                        "CAS basis failure - one or more basis keys were mutated since snapshot creation",
                    )),
                },
            },
        },
        Err(e) => Err(tonic::Status::internal(format!("Reply_tx closed | {}", e.to_string()))),
    }
}
#[tonic::async_trait]
impl proto::api_service_server::ApiService for Arc<Raft> {
    // id like these to all be macros but macroing gave me some weird lifetime complaint? i dont really understand how
    // maybe some weird macro_rules! interaction with async_trait

    async fn get(&self, req: tonic::Request<proto::GetReq>) -> tonic::Result<tonic::Response<proto::GetResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::GetResp = recv_read(self.submit_read_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn get_range(
        &self, req: tonic::Request<proto::GetRangeReq>,
    ) -> tonic::Result<tonic::Response<proto::GetRangeResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::GetRangeResp = recv_read(self.submit_read_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn get_batch(
        &self, req: tonic::Request<proto::GetBatchReq>,
    ) -> tonic::Result<tonic::Response<proto::GetBatchResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::GetBatchResp = recv_read(self.submit_read_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn list_keyspaces(
        &self, req: tonic::Request<proto::ListKeyspacesReq>,
    ) -> tonic::Result<tonic::Response<proto::ListKeyspacesResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::ListKeyspacesResp = recv_read(self.submit_read_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn list_snapshots(
        &self, req: tonic::Request<proto::ListSnapshotsReq>,
    ) -> tonic::Result<tonic::Response<proto::ListSnapshotsResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::ListSnapshotsResp = recv_read(self.submit_read_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn create_keyspace(
        &self, req: tonic::Request<proto::CreateKeyspaceReq>,
    ) -> tonic::Result<tonic::Response<proto::CreateKeyspaceResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::CreateKeyspaceResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn delete_keyspace(
        &self, req: tonic::Request<proto::DeleteKeyspaceReq>,
    ) -> tonic::Result<tonic::Response<proto::DeleteKeyspaceResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::DeleteKeyspaceResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn create_snapshot(
        &self, req: tonic::Request<proto::CreateSnapshotReq>,
    ) -> tonic::Result<tonic::Response<proto::CreateSnapshotResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::CreateSnapshotResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn delete_snapshot(
        &self, req: tonic::Request<proto::DeleteSnapshotReq>,
    ) -> tonic::Result<tonic::Response<proto::DeleteSnapshotResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::DeleteSnapshotResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn purge_snapshots(
        &self, req: tonic::Request<proto::PurgeSnapshotsReq>,
    ) -> tonic::Result<tonic::Response<proto::PurgeSnapshotsResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::PurgeSnapshotsResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn insert(&self, req: tonic::Request<proto::InsertReq>) -> tonic::Result<tonic::Response<proto::InsertResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::InsertResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn insert_batch(
        &self, req: tonic::Request<proto::InsertBatchReq>,
    ) -> tonic::Result<tonic::Response<proto::InsertBatchResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::InsertBatchResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn insert_batch_cas(
        &self, req: tonic::Request<proto::InsertBatchCasReq>,
    ) -> tonic::Result<tonic::Response<proto::InsertBatchCasResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::InsertBatchCasResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn delete(&self, req: tonic::Request<proto::DeleteReq>) -> tonic::Result<tonic::Response<proto::DeleteResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::DeleteResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn delete_range(
        &self, req: tonic::Request<proto::DeleteRangeReq>,
    ) -> tonic::Result<tonic::Response<proto::DeleteRangeResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::DeleteRangeResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    async fn delete_batch(
        &self, req: tonic::Request<proto::DeleteBatchReq>,
    ) -> tonic::Result<tonic::Response<proto::DeleteBatchResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::DeleteBatchResp = recv_write(self.submit_write_op(req.into())).await?.into();
        Ok(res.into())
    }

    //
}
