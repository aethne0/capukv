mod proto {
    tonic::include_proto!("capukv");
}

#[cfg(feature = "server")]
mod server {
    use crate::proto::*;

    use paste::paste;

    impl From<Err> for tonic::Status {
        fn from(value: Err) -> Self {
            match value {
                Err::SnapshotNotFound => tonic::Status::not_found("Snapshot".to_string()),
                Err::CasFailure => tonic::Status::aborted("CAS basis failure"),
            }
        }
    }

    impl From<Err> for ReadResp {
        fn from(value: Err) -> Self {
            ReadResp { read_resp: Some(read_resp::ReadResp::Err(value.into())) }
        }
    }

    impl From<Err> for WriteResp {
        fn from(value: Err) -> Self {
            WriteResp { write_resp: Some(write_resp::WriteResp::Err(value.into())) }
        }
    }

    #[inline]
    fn read_uuid_unwrap(b: &Vec<u8>) -> &uuid::Uuid {
        // Doesn't copy, just a uuid typed wrapper
        // Will panic if bytes isnt 16 length - i dont know how this would happen outside
        // protobufs not working properly or something more sinister
        uuid::Uuid::from_bytes_ref(b.as_slice().try_into().unwrap())
    }

    impl AppendEntriesRequest {
        #[inline]
        pub fn leader_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.leader_id)
        }
        #[inline]
        pub fn follower_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.follower_id)
        }
    }

    impl AppendEntriesResponse {
        #[inline]
        pub fn leader_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.leader_id)
        }
        #[inline]
        pub fn follower_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.follower_id)
        }
    }

    impl VoteRequest {
        #[inline]
        pub fn candidate_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.candidate_id)
        }
    }

    impl VoteResponse {
        #[inline]
        pub fn from_uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.from_id)
        }
    }

    impl LocalEntry {
        #[inline]
        pub fn voted_for_uuid(&self) -> Option<&uuid::Uuid> {
            match &self.voted_for {
                Some(b) => Some(read_uuid_unwrap(&b)),
                None => None,
            }
        }
        #[inline]
        pub fn uuid(&self) -> &uuid::Uuid {
            read_uuid_unwrap(&self.id)
        }
    }

    // ! these macros rely on correct protobuf naming!
    /*
    message ReadResp {
      oneof op {
        ListKeyspacesResp list_keyspaces_resp = 1;
    */
    // The CamelCase type name ( )must match the snake_case field name

    macro_rules! impl_read_op {
    ($name:ident) => {
        // from ReadOk
        paste! {
            impl From<read_ok::ReadOk> for [<$name Resp>] {
                fn from(value: read_ok::ReadOk) -> Self {
                    let read_ok::ReadOk::[<$name Resp>](value) = value else {
                        unreachable!(
                            "ReadOk -> Specific-Resp returned unexpected type. This should not happen and is probably our bug! | {:?}",
                            value
                        );
                    };
                    value
                }
            }
        }
        // req
        paste! {
            impl From<[<$name Req>]> for ReadOp {
                fn from(value: [<$name Req>]) -> Self {
                    ReadOp{ read_op: Some(read_op::ReadOp::[<$name Req>](value)) }
                }
            }
        }
        // resp
        paste! {
            impl From<[<$name Resp>]> for ReadResp {
                fn from(value: [<$name Resp>]) -> Self {
                    ReadResp {
                        read_resp: Some(read_resp::ReadResp::Ok(ReadOk {
                            read_ok: Some(read_ok::ReadOk::[<$name Resp>](value)),
                        })),
                    }
                }
            }
        }
    };
}

    macro_rules! impl_write_op {
    ($name:ident) => {
        // from ReadOk
        paste! {
            impl From<write_ok::WriteOk> for [<$name Resp>] {
                fn from(value: write_ok::WriteOk) -> Self {
                    let write_ok::WriteOk::[<$name Resp>](value) = value else {
                        unreachable!(
                            "WriteOk -> Specific-Resp returned unexpected type. This should not happen and is probably our bug! | {:?}",
                            value
                        );
                    };
                    value
                }
            }
        }
        // req
        paste! {
            impl From<[<$name Req>]> for WriteOp {
                fn from(value: [<$name Req>]) -> Self {
                    WriteOp { write_op: Some(write_op::WriteOp::[<$name Req>](value)) }
                }
            }
        }
        // resp
        paste! {
            impl From<[<$name Resp>]> for WriteResp {
                fn from(value: [<$name Resp>]) -> Self {
                    WriteResp {
                        write_resp: Some(write_resp::WriteResp::Ok(WriteOk {
                            write_ok: Some(write_ok::WriteOk::[<$name Resp>](value)),
                        })),
                    }
                }
            }
        }
    };
}

    impl_read_op!(ListSnapshots);
    impl_read_op!(Get);
    impl_read_op!(GetBatch);
    impl_read_op!(GetRange);

    impl_write_op!(CreateSnapshot);
    impl_write_op!(DeleteSnapshot);
    impl_write_op!(PurgeSnapshots);
    impl_write_op!(Insert);
    impl_write_op!(InsertBatch);
    impl_write_op!(InsertBatchCas);
    impl_write_op!(Delete);
    impl_write_op!(DeleteBatch);
    impl_write_op!(DeleteRange);

    impl From<(&AppendEntriesRequest, u64, bool)> for AppendEntriesResponse {
        fn from((req, term, success): (&AppendEntriesRequest, u64, bool)) -> Self {
            AppendEntriesResponse {
                term,
                success,
                req_id: req.req_id,
                leader_id: req.leader_id.clone(),
                follower_id: req.follower_id.clone(),
                prev_log_index: req.prev_log_index,
                prev_log_term: req.prev_log_term,
                leader_commit: req.leader_commit,
                first_entry_index: req.entries.first().map_or(0, |entry| entry.index),
                entries_len: req.entries.len() as u64,
            }
        }
    }
}

#[cfg(feature = "client")]
mod client {}

pub use proto::*;
