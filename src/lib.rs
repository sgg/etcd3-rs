use std::collections::HashMap;
use error::Result;

/// A blocking etcd client
mod sync_client;

/// An async etcd client built on tokio
///
/// This client is only used in the `apiserver`
mod client;

/// Auto-generated gRPC clients for etcd
pub mod grpc {
    pub mod authpb {
        tonic::include_proto!("authpb");
    }

    pub mod etcdserverpb {
        tonic::include_proto!("etcdserverpb");
    }

    pub mod mvccpb {
        tonic::include_proto!("mvccpb");
    }
}

/// Error types
pub mod error {
    /// An error raised by the etcd client
    #[derive(Debug, thiserror::Error)]
    pub enum Error {
        /// A gRPC status code raised by the underlying client
        #[error("gRPC Status - {0}")]
        Grpc(#[from] tonic::Status),
        /// An HTTP / transport error was raised
        #[error("Transport Error - {0}")]
        Transport(#[from] tonic::transport::Error),
        /// An I/O error was raised
        #[error("I/O Error - {0}")]
        Io(#[from] std::io::Error),
        /// Compare and swap failed for the key
        #[error("Compare and swap failed for key `{0}`")]
        Swap(String),
    }

    /// A result returned by an etcd client
    pub type Result<T> = std::result::Result<T, Error>;
}

pub mod prelude {
    pub use super::client::EtcdClient;

    pub use super::sync_client::SyncEtcdClient;

    pub use super::Etcd;
}

/// A type that implements an etcd client
pub trait Etcd {
    /// Store a value in etcd
    fn put(&mut self, k: impl AsRef<str>, v: impl AsRef<str>) -> Result<()>;

    /// Bulk load a set of keys into etcd
    fn bulk_put(&mut self, keys: &[impl AsRef<str>]) -> Result<()>;

    /// Retrieve a single key/value
    fn get(&mut self, k: impl AsRef<str>) -> Result<Option<String>>;

    /// Get all key/value pairs for the given prefix
    fn get_prefix(&mut self, prefix: impl AsRef<str>) -> Result<HashMap<String, String>>;

    /// Get all key/value pairs for the given prefix
    fn delete(&mut self, keys: &[impl AsRef<str>]) -> Result<()>;

    /// Delete all keys underneath a prefix atomically
    fn delete_prefix(&mut self, prefix: impl AsRef<str>) -> Result<()>;

    /// Perform an atomic compare and swap for a key
    fn swap(
        &mut self,
        k: impl AsRef<str>,
        old_v: impl AsRef<str>,
        new_v: impl AsRef<str>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
