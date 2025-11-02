use prost::Message;

use crate::proto::{self};

const LOCAL_KEY: [u8; 4] = *b"capu";
pub(crate) struct Persist {
    local_handle: fjall::PartitionHandle,
    /// After startup all reads can be done from this, which is in-memory.
    /// `save` must be called after writing to it.
    pub(crate) local: proto::LocalEntry,
}

impl Persist {
    /// This will generate an id if we dont have one yet as well
    pub(crate) fn new(id: String, path: &std::path::Path) -> Result<Self, crate::Error> {
        let db = fjall::Config::new(path).open()?;
        let local_handle = db.open_partition(
            "meta",
            fjall::PartitionCreateOptions::default()
                .compression(fjall::CompressionType::None)
                .block_size(monke::KiB!(1)),
        )?;

        let local_entry = match local_handle.get(&LOCAL_KEY)? {
            Some(slice) => {
                tracing::info!("Persistent state loaded ({})", monke::fmt_size(db.disk_space()));
                proto::LocalEntry::decode(&slice as &[u8])?
            }
            None => {
                tracing::info!("No persistent state found - initializing");
                let local_entry = proto::LocalEntry { id, term: 0, voted_for: None };
                local_handle.insert(&LOCAL_KEY, local_entry.encode_to_vec())?;
                local_entry
            }
        };

        Ok(Self { local_handle, local: local_entry })
    }

    pub(crate) fn save(&self) -> Result<(), crate::Error> {
        self.local_handle.insert(&LOCAL_KEY, self.local.encode_to_vec())?;
        Ok(())
    }
}
