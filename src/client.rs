use std::collections::HashMap;

use log::*;
use tonic::transport::Channel;
use tonic::Response;

use crate::grpc::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
use crate::grpc::etcdserverpb::kv_client::KvClient;
use crate::grpc::etcdserverpb::request_op::Request;
use crate::grpc::etcdserverpb::*;
use crate::error::{Error, Result};

const DEFAULT_ETCD_URL: &str = "http://localhost:2379";

/// A tokio based async client for etcd
///
/// This client wraps the tonic generated gRPC client and provides a more ergonomic interface
#[derive(Debug)]
pub struct EtcdClient {
    kv: KvClient<Channel>,
}

impl EtcdClient {
    /// Create a new client
    pub async fn new(endpoint: impl AsRef<str>) -> Result<Self> {
        let kv = KvClient::connect(endpoint.as_ref().to_string()).await?;
        Ok(Self { kv })
    }

    /// Create a client on localhost
    pub async fn localhost() -> Result<Self> {
        Self::new(DEFAULT_ETCD_URL).await
    }

    /// Store a value in etcd
    pub async fn put(&mut self, k: impl AsRef<str>, v: impl AsRef<str>) -> Result<()> {
        let req = PutRequest {
            key: k.as_ref().into(),
            value: v.as_ref().into(),
            ..PutRequest::default()
        };
        self.kv.put(req).await?;
        Ok(())
    }

    /// Bulk load a set of keys into etcd
    pub async fn bulk_put(&mut self, keys: &[impl AsRef<str>]) -> Result<()> {
        // n.b we chunk these operations as etcd has a limit on transaction size
        //  in production we probably wouldn't try to atomically insert thousands of keys at a time
        let chunks = keys.chunks(1000);
        for chunk in chunks {
            let success = chunk
                .iter()
                .map(|k| {
                    let k = k.as_ref();
                    RequestOp {
                        request: Some(Request::RequestPut(PutRequest {
                            key: k.as_bytes().to_vec(),
                            value: vec![],
                            lease: 0,
                            prev_kv: false,
                            ignore_value: false,
                            ignore_lease: false,
                        })),
                    }
                })
                .collect();
            let txn = TxnRequest {
                success,
                ..TxnRequest::default()
            };
            self.kv.txn(txn).await?;
        }

        Ok(())
    }

    pub async fn get(&mut self, k: impl AsRef<str>) -> Result<Option<String>> {
        let req = RangeRequest {
            key: k.as_ref().into(),
            ..RangeRequest::default()
        };
        let value =
            self.kv
                .range(req)
                .await
                .map(Response::into_inner)
                .map(|mut resp: RangeResponse| {
                    resp.kvs
                        .pop()
                        .map(|kv| String::from_utf8(kv.value).unwrap())
                })?;

        Ok(value)
    }

    /// Get all key/value pairs for the given prefix
    pub async fn get_prefix(&mut self, prefix: impl AsRef<str>) -> Result<HashMap<String, String>> {
        let key: Vec<u8> = prefix.as_ref().into();
        // n.b. etcd indicates prefix by setting range_end to key + 1
        let range_end = {
            let mut end = key.clone();
            if let Some(byte) = end.last_mut() {
                *byte += 1
            }
            end
        };

        let req = RangeRequest {
            key,
            range_end,
            ..RangeRequest::default()
        };

        let resp =
            self.kv
                .range(req)
                .await
                .map(Response::into_inner)
                .map(|resp: RangeResponse| {
                    resp.kvs
                        .into_iter()
                        .map(|kv| {
                            (
                                String::from_utf8(kv.key).unwrap(),
                                String::from_utf8(kv.value).unwrap(),
                            )
                        })
                        .collect::<HashMap<String, String>>()
                })?;
        trace!("Reply {:?}", resp);
        Ok(resp)
    }

    /// Delete a set of keys atomically
    pub async fn delete(&mut self, keys: &[impl AsRef<str>]) -> Result<()> {
        // create a Compare and RequestOp for each key
        debug!("Deleting {} keys", keys.len());

        let success = keys
            .iter()
            .map(AsRef::as_ref)
            .map(|key| {
                let key = key.as_bytes().to_vec();
                let range_end = vec![];
                RequestOp {
                    request: Some(Request::RequestDeleteRange(DeleteRangeRequest {
                        key,
                        range_end,
                        prev_kv: false,
                    })),
                }
            })
            .collect::<Vec<_>>();

        let txn = TxnRequest {
            compare: vec![],
            success,
            failure: vec![],
        };

        debug!("Beginning delete transaction...");
        let _resp = self.kv.txn(txn).await?;
        debug!("Transaction complete!");
        Ok(())
    }

    /// Delete all keys underneath a prefix atomically
    pub async fn delete_prefix(&mut self, prefix: impl AsRef<str>) -> Result<()> {
        let key: Vec<u8> = prefix.as_ref().into();
        // n.b. etcd indicates prefix by setting range_end to key + 1
        let range_end = {
            let mut end = key.clone();
            if let Some(byte) = end.last_mut() {
                *byte += 1;
            }
            end
        };

        let req = DeleteRangeRequest {
            key,
            range_end,
            prev_kv: false,
        };
        self.kv.delete_range(req).await?;

        Ok(())
    }

    /// Perform an atomic compare and swap for a key
    pub async fn swap(
        &mut self,
        k: impl AsRef<str>,
        old_v: impl AsRef<str>,
        new_v: impl AsRef<str>,
    ) -> Result<()> {
        let key = k.as_ref().as_bytes().to_vec();
        let old_v = old_v.as_ref().as_bytes().to_vec();
        let new_v = new_v.as_ref().as_bytes().to_vec();

        let compare = Compare {
            result: CompareResult::Equal as _,
            target: CompareTarget::Value as _,
            key: key.clone(),
            range_end: vec![],
            target_union: Some(TargetUnion::Value(old_v.clone())),
        };

        let op = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key,
                value: new_v,
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            })),
        };

        let txn = TxnRequest {
            compare: vec![compare],
            success: vec![op],
            failure: vec![],
        };

        let resp = self.kv.txn(txn).await?.into_inner();
        if resp.succeeded {
            Ok(())
        } else {
            Err(Error::Swap(k.as_ref().to_string()))
        }
    }
}
