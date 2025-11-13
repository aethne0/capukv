use std::sync::Arc;

use prost::Message;
use rust_rocksdb as rocksdb;
use rust_rocksdb::{Direction, IteratorMode};

use proto::LogEntry;

use crate::raft::db_open::LOG_HANDLE_NAME;

fn ser_key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

pub(crate) struct Log {
    db: Arc<rocksdb::DB>,
}

impl Log {
    /// This will generate an id if we dont have one yet as well
    pub(crate) fn new(db: Arc<rocksdb::DB>) -> Result<Self, crate::Error> {
        let handle = db.cf_handle(LOG_HANDLE_NAME).unwrap();
        let log_zero = LogEntry { term: 0, index: 0, op: None };
        db.put_cf(handle, ser_key(0), log_zero.encode_to_vec()).unwrap();

        Ok(Log { db })
    }

    pub(crate) fn candidates_log_up_to_date(&self, theirs: (u64, u64)) -> Result<bool, crate::Error> {
        Ok(theirs >= self.last_log_term_index()?)
    }

    /// (term, index)
    pub(crate) fn last_log_term_index(&self) -> Result<(u64, u64), crate::Error> {
        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();
        let mut iter = self.db.iterator_cf(handle, rust_rocksdb::IteratorMode::End);
        if let Some(Ok((_, value))) = iter.next() {
            let entry = LogEntry::decode(&value as &[u8])?;
            Ok((entry.term, entry.index))
        } else {
            Ok((0, 0))
        }
    }

    /// [index]
    pub(crate) fn get(&self, index: u64) -> Result<Option<LogEntry>, crate::Error> {
        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();
        if let Some(value) = self.db.get_cf(handle, ser_key(index)).unwrap() {
            let entry = LogEntry::decode(&value as &[u8])?;
            return Ok(Some(entry));
        }
        Ok(None)
    }

    /// Suffix of logs starting at and including index
    #[allow(unused)]
    pub(crate) fn get_suffix(&self, index: u64) -> Result<Vec<LogEntry>, crate::Error> {
        let mut collection = vec![];

        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();
        let mut iter = self.db.iterator_cf(handle, IteratorMode::From(&ser_key(index), Direction::Forward));

        while let Some(Ok(entry)) = iter.next() {
            collection.push(LogEntry::decode(&entry.1 as &[u8])?);
        }

        Ok(collection)
    }

    /// range of logs including start, excluding end like: `logs[start..end]`
    pub(crate) fn get_range(&self, start_index: u64, end_index: u64) -> Result<Vec<LogEntry>, crate::Error> {
        let mut collection = vec![];

        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();
        let mut iter = self.db.iterator_cf(handle, IteratorMode::From(&ser_key(start_index), Direction::Forward));

        while let Some(Ok((k, v))) = iter.next() {
            if k >= ser_key(end_index).into() {
                break;
            }
            collection.push(LogEntry::decode(&v as &[u8])?);
        }

        Ok(collection)
    }

    /// .last()
    pub(crate) fn last_index(&self) -> Result<u64, crate::Error> {
        Ok(self.last_log_term_index()?.1)
    }

    /// Deletes `log[last_index]` and all entries after it
    pub(crate) async fn truncate(&self, last_index: u64) -> Result<(), crate::Error> {
        // surely theres a faster way to do this, or itd be an easy addition to fjall
        // todo do this more efficiently ^
        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();

        let mut keys = vec![];

        let mut iter = self.db.iterator_cf(handle, IteratorMode::From(&ser_key(last_index), Direction::Forward));
        while let Some(Ok((k, _))) = iter.next() {
            keys.push(k);
        }

        for key in keys {
            self.db.delete_cf(handle, key).unwrap();
        }

        Ok(())
    }

    /// Inserts at entry.index - can overwrite. Does not check for contiguity (entry at index-1)
    pub(crate) fn insert(&self, entry: &LogEntry) -> Result<(), crate::Error> {
        let handle = self.db.cf_handle(LOG_HANDLE_NAME).unwrap();
        let mut wopts = rocksdb::WriteOptions::default();
        wopts.set_sync(true);
        self.db.put_cf_opt(handle, ser_key(entry.index), entry.encode_to_vec(), &wopts).unwrap();
        Ok(())
    }
}
