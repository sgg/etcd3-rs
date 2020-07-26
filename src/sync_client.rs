use std::collections::HashMap;

use crate::client::*;
use crate::{Etcd, Result};

/// A synchronous version of the [`EtcdClient`]
///
/// This simply wraps the `EtcdClient` and pairs it with an embedded tokio runtime
pub struct SyncEtcdClient {
    inner: EtcdClient,
    rt: tokio::runtime::Runtime,
}

impl SyncEtcdClient {
    /// Create a new client
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self> {
        let mut rt = tokio::runtime::Runtime::new()?;
        let inner = rt.block_on(async { EtcdClient::new(endpoint).await })?;
        Ok(Self { inner, rt })
    }

    /// Create a client on localhost
    pub fn localhost() -> Result<Self> {
        let mut rt = tokio::runtime::Runtime::new()?;
        let inner = rt.block_on(async { EtcdClient::localhost().await })?;

        Ok(Self { inner, rt })
    }
}

impl Etcd for SyncEtcdClient {

    /// Store a value in etcd
    fn put(&mut self, k: impl AsRef<str>, v: impl AsRef<str>) -> Result<()> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.put(k, v).await })
    }

    /// Bulk load a set of keys into etcd
    fn bulk_put(&mut self, keys: &[impl AsRef<str>]) -> Result<()> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.bulk_put(keys).await })
    }

    /// Retrieve a single key/value
    fn get(&mut self, k: impl AsRef<str>) -> Result<Option<String>> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.get(k).await })
    }

    /// Get all key/value pairs for the given prefix
    fn get_prefix(&mut self, prefix: impl AsRef<str>) -> Result<HashMap<String, String>> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.get_prefix(prefix).await })
    }

    /// Delete a set of keys atomically
    fn delete(&mut self, keys: &[impl AsRef<str>]) -> Result<()> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.delete(keys).await })
    }

    /// Delete all keys underneath a prefix atomically
    fn delete_prefix(&mut self, prefix: impl AsRef<str>) -> Result<()> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.delete_prefix(prefix).await })
    }

    /// Perform an atomic compare and swap for a key
    fn swap(
        &mut self,
        k: impl AsRef<str>,
        old_v: impl AsRef<str>,
        new_v: impl AsRef<str>,
    ) -> Result<()> {
        let Self { inner, rt } = self;
        rt.block_on(async { inner.swap(k, old_v, new_v).await })
    }
}

#[cfg(test)]
mod tests {
    // TODO(tests): Implement integration tests for etcd client
}
