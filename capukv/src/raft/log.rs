use prost::Message;
use rust_rocksdb::{DB, Direction, IteratorMode, Options};

use crate::proto::LogEntry;

fn ser_key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

pub(crate) struct Log {
    logs_handle: DB,
}

impl Log {
    /// This will generate an id if we dont have one yet as well
    pub(crate) fn new(path: &std::path::Path) -> Result<Self, crate::Error> {
        let mut opt = Options::default();
        opt.create_if_missing(true);
        let logs_handle = DB::open(&opt, path).unwrap();

        //let db = fjall::Config::new(path).open()?;
        //let logs_handle = db.open_partition("logs", fjall::PartitionCreateOptions::default())?;

        // ensure log entry 0 exists
        let log_zero = LogEntry { term: 0, index: 0, op: None };
        logs_handle.put(ser_key(0), log_zero.encode_to_vec()).unwrap();

        Ok(Log { logs_handle })
    }

    pub(crate) fn candidates_log_up_to_date(&self, theirs: (u64, u64)) -> Result<bool, crate::Error> {
        Ok(theirs >= self.last_log_term_index()?)
    }

    /// (term, index)
    pub(crate) fn last_log_term_index(&self) -> Result<(u64, u64), crate::Error> {
        let mut iter = self.logs_handle.iterator(rust_rocksdb::IteratorMode::End);
        if let Some(Ok((_, value))) = iter.next() {
            let entry = LogEntry::decode(&value as &[u8])?;
            Ok((entry.term, entry.index))
        } else {
            Ok((0, 0))
        }
    }

    /// [index]
    pub(crate) fn get(&self, index: u64) -> Result<Option<LogEntry>, crate::Error> {
        if let Some(value) = self.logs_handle.get(ser_key(index)).unwrap() {
            let entry = LogEntry::decode(&value as &[u8])?;
            return Ok(Some(entry));
        }
        Ok(None)
    }

    /// Suffix of logs starting at and including index
    #[allow(unused)]
    pub(crate) fn get_suffix(&self, index: u64) -> Result<Vec<LogEntry>, crate::Error> {
        let mut collection = vec![];

        let mut iter = self.logs_handle.iterator(IteratorMode::From(&ser_key(index), Direction::Forward));

        while let Some(Ok(entry)) = iter.next() {
            collection.push(LogEntry::decode(&entry.1 as &[u8])?);
        }

        Ok(collection)
    }

    /// range of logs including start, excluding end like: `logs[start..end]`
    pub(crate) fn get_range(&self, start_index: u64, end_index: u64) -> Result<Vec<LogEntry>, crate::Error> {
        let mut collection = vec![];

        let mut iter = self.logs_handle.iterator(IteratorMode::From(&ser_key(start_index), Direction::Forward));

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

        let mut keys = vec![];

        let mut iter = self.logs_handle.iterator(IteratorMode::From(&ser_key(last_index), Direction::Forward));
        while let Some(Ok((k, _))) = iter.next() {
            keys.push(k);
        }

        for key in keys {
            self.logs_handle.delete(key).unwrap();
        }

        Ok(())
    }

    /// Inserts at entry.index - can overwrite. Does not check for contiguity (entry at index-1)
    pub(crate) fn insert(&self, entry: &LogEntry) -> Result<(), crate::Error> {
        self.logs_handle.put(ser_key(entry.index), entry.encode_to_vec()).unwrap();
        Ok(())
    }
}
