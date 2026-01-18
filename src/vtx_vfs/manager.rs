use std::collections::HashMap;
use std::sync::RwLock;

use object_store::path::Path as ObjectPath;

use super::entry::VtxVfsStoreEntry;

mod normalize;
mod ops;
mod resolve;

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct VfsResolved {
    entry: VtxVfsStoreEntry,
    location: Option<ObjectPath>,
}

pub struct VfsObject {
    pub uri: String,
    pub size: u64,
    pub last_modified: Option<i64>,
    pub etag: Option<String>,
}

pub struct VtxVfsManager {
    stores: RwLock<HashMap<String, VtxVfsStoreEntry>>,
}

fn _assert_send_sync() {
    fn assert<T: Send + Sync>() {}
    assert::<VtxVfsManager>();
}

impl VtxVfsManager {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            stores: RwLock::new(HashMap::new()),
        })
    }
}
