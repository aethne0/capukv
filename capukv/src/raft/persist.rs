use std::sync::Arc;

use prost::Message;
use rust_rocksdb as rocksdb;

const LOCAL_KEY: [u8; 4] = *b"capu";

pub(crate) struct Persist {
    db: Arc<rocksdb::DB>,
    /// After startup all reads can be done from this, which is in-memory.
    /// `save` must be called after writing to it.
    pub(crate) local: proto::LocalEntry,
}

impl Persist {
    /// This will generate an id if we dont have one yet as well
    pub(crate) fn new(id: String, db: Arc<rocksdb::DB>) -> Result<Self, crate::Error> {
        let handle = db.cf_handle("local").unwrap();

        let local_entry = match db.get_cf(&handle, &LOCAL_KEY)? {
            Some(slice) => {
                tracing::info!("Persistent state loaded");
                proto::LocalEntry::decode(&slice as &[u8])?
            }
            None => {
                tracing::info!("No persistent state found - initializing");
                let local_entry = proto::LocalEntry { id, term: 0, voted_for: None };
                db.put_cf(handle, &LOCAL_KEY, local_entry.encode_to_vec())?;
                local_entry
            }
        };

        Ok(Self { db, local: local_entry })
    }

    pub(crate) fn save(&self) -> Result<(), crate::Error> {
        let handle = self.db.cf_handle("local").unwrap();
        let mut wopt = rocksdb::WriteOptions::default();
        wopt.set_sync(true);
        self.db.put_cf_opt(handle, &LOCAL_KEY, self.local.encode_to_vec(), &wopt)?;
        Ok(())
    }
}
