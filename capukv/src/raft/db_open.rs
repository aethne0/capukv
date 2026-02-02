use std::sync::Arc;

use rust_rocksdb as rocksdb;

pub(crate) const LOCAL_HANDLE_NAME: &'static str = "local";
pub(crate) const LOG_HANDLE_NAME: &'static str = "log";

pub(crate) fn open_db(path: &std::path::Path) -> Arc<rocksdb::DB> {
    let mut opt = rocksdb::Options::default();
    opt.create_if_missing(true);
    opt.create_missing_column_families(true);

    let mut local_opts = rocksdb::Options::default();
    local_opts.set_compression_type(rocksdb::DBCompressionType::None);
    local_opts.set_write_buffer_size(0x400);
    local_opts.set_max_background_jobs(0);
    local_opts.increase_parallelism(1);

    let cfs = vec![
        rocksdb::ColumnFamilyDescriptor::new(LOCAL_HANDLE_NAME, rocksdb::Options::default()),
        rocksdb::ColumnFamilyDescriptor::new(LOG_HANDLE_NAME, rocksdb::Options::default()),
    ];
    let db = Arc::new(rocksdb::DB::open_cf_descriptors(&opt, path, cfs).expect("Couldn't open DB"));

    db
}
