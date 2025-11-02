use std::{collections::BTreeMap, sync::Arc};

use im::OrdMap;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    sync::{Mutex, RwLock, mpsc, oneshot},
    time::Instant,
};

use crate::{proto, raft::log::Log};

pub(crate) struct StateMachine {
    inner: Arc<RwLock<Inner>>,
    tx: mpsc::Sender<u64>,
    cmd_reply_txs: Arc<Mutex<BTreeMap<u64, oneshot::Sender<proto::WriteResp>>>>,
}

impl StateMachine {
    #[must_use]
    pub(crate) fn new(log: Arc<Log>) -> Self {
        let (tx, mut rx) = mpsc::channel::<u64>(4);

        let inner = Arc::new(RwLock::new(Inner {
            commit_index: 0,
            last_applied: 0,
            keyspaces: FxHashMap::default(),
            prev_keyspace: 0,
        }));
        let cmd_reply_txs = Arc::new(Mutex::new(BTreeMap::new()));

        tokio::spawn({
            let inner = inner.clone();
            let cmd_reply_txs = cmd_reply_txs.clone();
            // state machine "worker" that applies commited log entries asynchronously
            let log = log.clone();
            async move {
                while let Some(new_commit_index) = rx.recv().await {
                    {
                        let mut wlock = inner.write().await;
                        wlock.commit_index = wlock.commit_index.max(new_commit_index);
                    }

                    while inner.read().await.last_applied < new_commit_index {
                        let last_applied = inner.read().await.last_applied + 1;

                        match log.get(last_applied) {
                            Err(e) => panic!("State machine apply task couldn't read from log - {:?}", e),

                            Ok(entry) => {
                                // If this panics one of the following happened:
                                //  1. We incorrectly increased our commit-index\/n (our bug)
                                //  2. We failed to add entries to our log at some point, or in some way\n (our bug)
                                //  3. We had a persistence error")
                                let entry = entry.expect("Tried to apply an entry we don't have in our log");

                                tracing::trace!("Applying entry: {}", entry.index);

                                let mut wlock = inner.write().await;
                                let reply_tx = cmd_reply_txs.lock().await.remove(&entry.index);
                                wlock.apply_mutation(entry, reply_tx).await;
                            }
                        }
                    }
                }
            }
        });

        Self { inner, tx, cmd_reply_txs }
    }

    pub(crate) async fn commit_index(&self) -> u64 {
        self.inner.read().await.commit_index
    }

    #[allow(unused)]
    pub(crate) async fn last_applied(&self) -> u64 {
        self.inner.read().await.last_applied
    }

    /// This increases our commit index, which can cause a worker to start applying entries to
    /// the state machine (if any). This will increase increase last_applied and of course mutate
    /// the state.
    pub(crate) async fn set_commit_index(&self, commit_index: u64) {
        self.tx.send(commit_index).await.unwrap()
    }

    pub(crate) async fn add_op_reply_tx(&self, index: u64, reply_tx: oneshot::Sender<proto::WriteResp>) {
        self.cmd_reply_txs.lock().await.insert(index, reply_tx);
    }

    /// To be called on term change - we might have ids that get truncated and rewritten
    /// So other wise we might be a follower but reply to a stale request as if we were still leader
    /// (And probably with/from unrelated data)
    pub(crate) async fn clear_op_reply_txs(&self) {
        self.cmd_reply_txs.lock().await.clear();
    }

    pub(crate) async fn read_op(&self, read_op: proto::ReadOp) -> proto::ReadResp {
        // todo make locks operate at snapshot/keyspace level
        // wont matter til we implement sloppy reads though
        self.inner.write().await.read_op(read_op).await
    }
}

type Key = Vec<u8>;
type Value = Vec<u8>;

#[derive(Debug)]
struct Snapshot {
    store: OrdMap<Key, Value>,

    /// Used for CAS/SSI - we will compare these to the main-store at time of writing
    /// CAS writes will take a "basis" snapshot id so that we can check at time of commit (todo)
    read_keys: FxHashSet<Key>,

    _last_applied: u64,
    _created: Instant,
}
impl Snapshot {
    fn new(store_image: OrdMap<Key, Value>, last_applied: u64) -> Self {
        Self {
            store: store_image,
            read_keys: FxHashSet::default(),
            _created: Instant::now(),
            _last_applied: last_applied,
        }
    }
}

struct Keyspace {
    /// Map<Key,value>
    store: OrdMap<Key, Value>,
    _created: Instant,

    snapshots: FxHashMap<u64, Snapshot>,
    prev_snapshot_id: u64,
}
impl Keyspace {
    fn new() -> Self {
        Self { store: OrdMap::new(), _created: Instant::now(), snapshots: FxHashMap::default(), prev_snapshot_id: 0 }
    }
}

struct Inner {
    /// keyspace_id -> Map<Key, Value>
    prev_keyspace: u64,
    keyspaces: FxHashMap<u64, Keyspace>,

    commit_index: u64,
    last_applied: u64,
    // this will be ordered, or else we did something wrong.
}

impl Inner {
    async fn apply_mutation(&mut self, entry: proto::LogEntry, reply_tx: Option<oneshot::Sender<proto::WriteResp>>) {
        tracing::trace!("APPLYING: {} (LA:{} CI:{})", entry.index, self.last_applied, self.commit_index,);

        // ya...
        let Some(op) = entry.op else { unreachable!() };

        let resp = if let Some(op) = op.write_op {
            use proto::write_op::WriteOp::*;
            let resp: proto::WriteResp = match op {
                CreateKeyspaceReq(proto::CreateKeyspaceReq {}) => {
                    self.prev_keyspace += 1;
                    self.keyspaces.insert(self.prev_keyspace, Keyspace::new());
                    proto::CreateKeyspaceResp { keyspace: self.prev_keyspace }.into()
                }
                DeleteKeyspaceReq(proto::DeleteKeyspaceReq { keyspace }) => match self.keyspaces.remove(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(_ks) => proto::DeleteKeyspaceResp {}.into(),
                },

                CreateSnapshotReq(proto::CreateSnapshotReq { keyspace }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        ks.prev_snapshot_id += 1;
                        ks.snapshots.insert(ks.prev_snapshot_id, Snapshot::new(ks.store.clone(), self.last_applied));
                        proto::CreateSnapshotResp { snapshot: ks.prev_snapshot_id }.into()
                    }
                },
                DeleteSnapshotReq(proto::DeleteSnapshotReq { keyspace, snapshot }) => {
                    match self.keyspaces.get_mut(&keyspace) {
                        None => proto::Err::KeyspaceNotFound.into(),
                        Some(ks) => match ks.snapshots.remove(&snapshot) {
                            Some(_ss) => proto::DeleteSnapshotResp {}.into(),
                            None => proto::Err::SnapshotNotFound.into(),
                        },
                    }
                }
                PurgeSnapshotsReq(proto::PurgeSnapshotsReq { keyspace }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        let count = ks.snapshots.len() as u64;
                        ks.snapshots.clear();
                        proto::PurgeSnapshotsResp { count }.into()
                    }
                },

                InsertReq(proto::InsertReq { keyspace, key, value }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        let old_value = ks.store.insert(key, value);
                        proto::InsertResp { old_value }.into()
                    }
                },

                InsertBatchReq(proto::InsertBatchReq { keyspace, pairs }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        let mut old_pairs = vec![];

                        for proto::Pair { key, value } in pairs {
                            if let Some(old) = ks.store.insert(key.clone(), value) {
                                old_pairs.push(proto::Pair { key, value: old });
                            }
                        }

                        proto::InsertBatchResp { old_pairs }.into()
                    }
                },

                InsertBatchCasReq(proto::InsertBatchCasReq { keyspace, snapshot, pairs }) => {
                    match self.keyspaces.get_mut(&keyspace) {
                        None => proto::Err::KeyspaceNotFound.into(),
                        Some(ks) => match ks.snapshots.get(&snapshot) {
                            None => proto::Err::SnapshotNotFound.into(),
                            Some(ss) => match ss.read_keys.iter().any(|k| ss.store.get(k) != ks.store.get(k)) {
                                // if snapshot read-basis was violated
                                true => proto::Err::CasFailure.into(),
                                // otherwise we can write
                                false => {
                                    let mut old_pairs = vec![];
                                    for proto::Pair { key, value } in pairs {
                                        if let Some(old) = ks.store.insert(key.clone(), value) {
                                            old_pairs.push(proto::Pair { key, value: old });
                                        }
                                    }

                                    proto::InsertBatchCasResp { old_pairs }.into()
                                }
                            },
                        },
                    }
                }

                DeleteReq(proto::DeleteReq { keyspace, key }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        let old_value = ks.store.remove(&key);
                        proto::DeleteResp { old_value }.into()
                    }
                },

                DeleteBatchReq(proto::DeleteBatchReq { keyspace, keys }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        let mut old_pairs = vec![];

                        for k in keys {
                            if let Some((key, value)) = ks.store.remove_with_key(&k) {
                                old_pairs.push(proto::Pair { key, value });
                            }
                        }

                        proto::DeleteBatchResp { old_pairs }.into()
                    }
                },

                DeleteRangeReq(proto::DeleteRangeReq { keyspace, start_key, end_key }) => {
                    match self.keyspaces.get_mut(&keyspace) {
                        None => proto::Err::KeyspaceNotFound.into(),
                        Some(ks) => {
                            let mut old_pairs = vec![];

                            // Probably have to clone because its immutable, don't think theres a way around this
                            // Normally we could (conceptually) do something like
                            // ks.store.range(..).drain(..)
                            let to_remove: Vec<Vec<u8>> =
                                ks.store.range(start_key..end_key).map(|(k, _)| k.clone()).collect();

                            for k in to_remove {
                                if let Some((key, value)) = ks.store.remove_with_key(&k) {
                                    old_pairs.push(proto::Pair { key, value });
                                }
                            }

                            proto::DeleteRangeResp { old_pairs }.into()
                        }
                    }
                }
            };

            resp
        } else {
            proto::WriteResp { write_resp: None }
        };

        assert_eq!(self.last_applied + 1, entry.index);
        self.last_applied = entry.index;

        if let Some(reply_tx) = reply_tx {
            let _ = reply_tx.send(resp);
        }
    }

    pub(crate) async fn read_op(&mut self, read_op: proto::ReadOp) -> proto::ReadResp {
        use proto::read_op::ReadOp::*;

        // todo depuplicate - just writing these quick now

        match read_op.read_op {
            None => unreachable!(),
            Some(op) => match op {
                GetReq(proto::GetReq { keyspace, snapshot, key }) => match self.keyspaces.get_mut(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => match snapshot {
                        Some(requested_ss) => match ks.snapshots.get_mut(&requested_ss) {
                            None => proto::Err::SnapshotNotFound.into(),
                            Some(ss) => {
                                let value = ss.store.get(&key).cloned();
                                tracing::info!("ZZZ {:?}", &key);
                                ss.read_keys.insert(key);
                                proto::GetResp { value }.into()
                            }
                        },
                        None => {
                            let value = ks.store.get(&key).cloned();
                            proto::GetResp { value }.into()
                        }
                    },
                },

                GetBatchReq(proto::GetBatchReq { keyspace, snapshot, keys }) => match self.keyspaces.get_mut(&keyspace)
                {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => match snapshot {
                        Some(requested_ss) => match ks.snapshots.get_mut(&requested_ss) {
                            None => proto::Err::SnapshotNotFound.into(),
                            Some(ss) => {
                                let mut pairs = vec![];
                                for key in keys {
                                    if let Some((k, v)) = ss.store.get_key_value(&key) {
                                        ss.read_keys.insert(k.clone());
                                        pairs.push(proto::Pair { key: k.clone(), value: v.clone() });
                                    }
                                }
                                proto::GetBatchResp { pairs }.into()
                            }
                        },
                        None => {
                            let mut pairs = vec![];
                            for key in keys {
                                if let Some((k, v)) = ks.store.get_key_value(&key) {
                                    pairs.push(proto::Pair { key: k.clone(), value: v.clone() });
                                }
                            }
                            proto::GetBatchResp { pairs }.into()
                        }
                    },
                },

                GetRangeReq(proto::GetRangeReq { keyspace, snapshot, start_key, end_key }) => {
                    match self.keyspaces.get_mut(&keyspace) {
                        None => proto::Err::KeyspaceNotFound.into(),
                        Some(ks) => match snapshot {
                            Some(requested_ss) => match ks.snapshots.get_mut(&requested_ss) {
                                None => proto::Err::SnapshotNotFound.into(),
                                Some(ss) => {
                                    let mut pairs = vec![];

                                    for (k, v) in ss.store.range(start_key..end_key) {
                                        ss.read_keys.insert(k.clone());
                                        pairs.push(proto::Pair { key: k.clone(), value: v.clone() });
                                    }
                                    proto::GetRangeResp { pairs }.into()
                                }
                            },

                            None => {
                                let mut pairs = vec![];

                                for (k, v) in ks.store.range(start_key..end_key) {
                                    pairs.push(proto::Pair { key: k.clone(), value: v.clone() });
                                }
                                proto::GetRangeResp { pairs }.into()
                            }
                        },
                    }
                }

                ListKeyspacesReq(proto::ListKeyspacesReq {}) => {
                    let keyspaces = self.keyspaces.keys().cloned().collect();
                    proto::ListKeyspacesResp { keyspaces }.into()
                }

                ListSnapshotsReq(proto::ListSnapshotsReq { keyspace }) => match self.keyspaces.get(&keyspace) {
                    None => proto::Err::KeyspaceNotFound.into(),
                    Some(ks) => {
                        for ss in &ks.snapshots {
                            tracing::debug!("{:#?}", ss);
                        }
                        let snapshots = ks.snapshots.keys().cloned().collect();
                        proto::ListSnapshotsResp { snapshots }.into()
                    }
                },
            },
        }
    }
}
