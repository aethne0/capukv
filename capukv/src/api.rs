// ? status.rs in the tonic code is a great guide to which grpc status-codes are correct
// ? some relevent excerpts:
// Operation was rejected because the system is not in a state required for
// the operation's execution. For example, directory to be deleted may be
// non-empty, an rmdir operation is applied to a non-directory, etc.
//
// A litmus test that may help a service implementor in deciding between
// `FailedPrecondition`, `Aborted`, and `Unavailable`:
// (a) Use `Unavailable` if the client can retry just the failing call.
// (b) Use `Aborted` if the client should retry at a higher-level (e.g.,
//     restarting a read-modify-write sequence).
// (c) Use `FailedPrecondition` if the client should not retry until the
//     system state has been explicitly fixed.  E.g., if an "rmdir" fails
//     because the directory is non-empty, `FailedPrecondition` should be
//     returned since the client should not retry unless they have first
//     fixed up the directory by deleting files from it.

impl From<crate::err::RaftResponseError> for tonic::Status {
    fn from(value: crate::err::RaftResponseError) -> Self {
        match value {
            crate::err::RaftResponseError::Fail { msg } => tonic::Status::internal(format!("{}", msg)),
            // todo leader messaging
            crate::err::RaftResponseError::NotLeader { uri } => tonic::Status::failed_precondition(format!("{}", uri)),
            crate::err::RaftResponseError::NotReady => {
                tonic::Status::unavailable("Leader unknown OR no majority".to_string())
            }
            // todo probably wrong code for this
            crate::err::RaftResponseError::Timeout => tonic::Status::deadline_exceeded("Timed out".to_string()),
            // todo i think this neccessarily means the operation did not happen, check
            crate::err::RaftResponseError::OperationCancelled => {
                tonic::Status::unavailable("Operation cancelled - node turned into follower or something")
            }
        }
    }
}

async fn recv_write(
    rx: impl Future<Output = Result<proto::WriteResp, crate::err::RaftResponseError>>,
) -> Result<proto::write_ok::WriteOk, tonic::Status> {
    let resp = rx.await;
    tracing::debug!("WriteOp Resp: {:?}", &resp);
    match resp {
        Err(e) => Err(e.into()),
        Ok(val) => match val.write_resp.unwrap() {
            proto::write_resp::WriteResp::Ok(val) => Ok(val.write_ok.unwrap()),
            proto::write_resp::WriteResp::Err(err) => Err(proto::Err::try_from(err).unwrap().into()),
        },
    }
}

async fn recv_read(
    rx: impl Future<Output = Result<proto::ReadResp, crate::err::RaftResponseError>>,
) -> Result<proto::read_ok::ReadOk, tonic::Status> {
    let resp = rx.await;
    tracing::debug!("ReadOp Resp: {:?}", &resp);
    match resp {
        Err(e) => Err(e.into()),
        Ok(val) => match val.read_resp.unwrap() {
            proto::read_resp::ReadResp::Ok(val) => Ok(val.read_ok.unwrap()),
            proto::read_resp::ReadResp::Err(err) => Err(proto::Err::try_from(err).unwrap().into()),
        },
    }
}

#[tonic::async_trait]
impl proto::api_service_server::ApiService for &'static crate::raft::Raft {
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

    async fn list_snapshots(
        &self, req: tonic::Request<proto::ListSnapshotsReq>,
    ) -> tonic::Result<tonic::Response<proto::ListSnapshotsResp>> {
        let (_, _, req) = req.into_parts();
        let res: proto::ListSnapshotsResp = recv_read(self.submit_read_op(req.into())).await?.into();
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
