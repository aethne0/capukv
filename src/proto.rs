use paste::paste;

tonic::include_proto!("capukv");

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

impl_read_op!(ListKeyspaces);
impl_read_op!(ListSnapshots);
impl_read_op!(Get);
impl_read_op!(GetBatch);
impl_read_op!(GetRange);

impl_write_op!(CreateKeyspace);
impl_write_op!(DeleteKeyspace);
impl_write_op!(CreateSnapshot);
impl_write_op!(DeleteSnapshot);
impl_write_op!(PurgeSnapshots);
impl_write_op!(Insert);
impl_write_op!(InsertBatch);
impl_write_op!(InsertBatchCas);
impl_write_op!(Delete);
impl_write_op!(DeleteBatch);
impl_write_op!(DeleteRange);
