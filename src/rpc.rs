use crate::archive::load_head_manifest;
use crate::cache::SegmentCache;
use crate::format::{
    data32, parse_quantity, quantity, quantity_u256, AccountEvent, Address, Hash32, Manifest,
    SegmentDescriptor, SegmentIndex,
};
use crate::store::ArchiveStore;
use anyhow::{anyhow, bail, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

pub struct Publication {
    pub manifest_digest: Hash32,
    pub manifest: Manifest,
    descriptors: Vec<(SegmentDescriptor, SegmentIndex)>,
    cache: SegmentCache,
}

impl Publication {
    pub async fn load(
        store: Arc<dyn ArchiveStore>,
        chain_id: u64,
        cache_dir: PathBuf,
        memory_mib: u64,
    ) -> Result<Self> {
        let (_, manifest_digest, manifest) = load_head_manifest(store.as_ref(), chain_id).await?;
        let cache = SegmentCache::new(store, cache_dir, memory_mib)?;
        let mut descriptors = Vec::with_capacity(manifest.segments.len());
        for descriptor in &manifest.segments {
            let index = cache.mirror_index(descriptor).await?;
            descriptors.push((descriptor.clone(), index));
        }
        Ok(Self {
            manifest_digest,
            manifest,
            descriptors,
            cache,
        })
    }

    pub async fn account_at(&self, address: Address, block: u64) -> Result<Option<AccountEvent>> {
        self.require_block(block)?;
        let key = address.to_string();
        for (descriptor, index) in self.descriptors.iter().rev() {
            if descriptor.start_block > block || !index_has(index, "account", &key, block) {
                continue;
            }
            let segment = self.cache.segment(descriptor).await?;
            if let Some(event) = segment
                .accounts
                .iter()
                .rev()
                .find(|event| event.address == address && event.block <= block)
            {
                return Ok(event.exists.then_some(event.clone()));
            }
        }
        Ok(None)
    }

    pub async fn storage_at(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok([0; 32]);
        };
        let key = format!("{}:{:016x}:{}", address, account.incarnation, slot);
        for (descriptor, index) in self.descriptors.iter().rev() {
            if descriptor.start_block > block || !index_has(index, "storage", &key, block) {
                continue;
            }
            let segment = self.cache.segment(descriptor).await?;
            if let Some(event) = segment.storage.iter().rev().find(|event| {
                event.address == address
                    && event.incarnation == account.incarnation
                    && event.slot == slot
                    && event.block <= block
            }) {
                return Ok(event.value);
            }
        }
        Ok([0; 32])
    }

    pub async fn code_at(&self, address: Address, block: u64) -> Result<Vec<u8>> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok(Vec::new());
        };
        let key = account.code_hash.to_string();
        for (descriptor, index) in self.descriptors.iter().rev() {
            if !index_has(index, "code", &key, block) {
                continue;
            }
            let segment = self.cache.segment(descriptor).await?;
            if let Some(blob) = segment
                .code
                .iter()
                .find(|blob| blob.code_hash == account.code_hash && blob.first_seen_block <= block)
            {
                return Ok(blob.bytes.clone());
            }
        }
        Ok(Vec::new())
    }

    fn require_block(&self, block: u64) -> Result<()> {
        if block < self.manifest.anchor_number || block > self.manifest.published_number {
            bail!("block unavailable");
        }
        Ok(())
    }

    fn resolve_selector(&self, selector: &Value) -> Result<u64> {
        match selector {
            Value::String(tag) => match tag.as_str() {
                "latest" => Ok(self.manifest.published_number),
                "safe" | "finalized"
                    if self.manifest.publication_gate.starts_with("fixed-offset:") =>
                {
                    bail!("block unavailable: fixed-offset publication is not safe or finalized")
                }
                "safe" | "finalized" => Ok(self.manifest.published_number),
                "earliest" if self.manifest.anchor_number == 0 => Ok(0),
                "earliest" => bail!("block unavailable"),
                "pending" => bail!("pending is unsupported"),
                number => {
                    let number = parse_quantity(number)?;
                    self.require_block(number)?;
                    Ok(number)
                }
            },
            Value::Object(object) => {
                if let Some(number) = object.get("blockNumber") {
                    if object.contains_key("blockHash") {
                        bail!("selector cannot contain blockNumber and blockHash");
                    }
                    return self.resolve_selector(number);
                }
                let hash = object
                    .get("blockHash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("invalid EIP-1898 selector"))?;
                let hash = Hash32::from_str(hash)?;
                let _require_canonical = object
                    .get("requireCanonical")
                    .map(|value| {
                        value
                            .as_bool()
                            .ok_or_else(|| anyhow!("requireCanonical must be boolean"))
                    })
                    .transpose()?;
                self.block_number_by_hash(hash)
                    .ok_or_else(|| anyhow!("block unavailable"))
            }
            _ => bail!("invalid block selector"),
        }
    }

    fn block_number_by_hash(&self, hash: Hash32) -> Option<u64> {
        let suffix = format!(":{hash}");
        self.descriptors
            .iter()
            .flat_map(|(_, index)| &index.entries)
            .find_map(|entry| {
                (entry.namespace == "block" && entry.key.ends_with(&suffix))
                    .then_some(entry.min_block)
            })
    }

    pub fn metrics(&self) -> String {
        let archive_bytes: u64 = self
            .manifest
            .segments
            .iter()
            .map(|segment| segment.segment_size + segment.index_size)
            .sum();
        format!(
            concat!(
                "fossil_publication_generation {}\n",
                "fossil_published_block {}\n",
                "fossil_archive_segments {}\n",
                "fossil_archive_object_bytes {}\n",
                "{}"
            ),
            self.manifest.generation,
            self.manifest.published_number,
            self.manifest.segments.len(),
            archive_bytes,
            self.cache.stats.render()
        )
    }
}

fn index_has(index: &SegmentIndex, namespace: &str, key: &str, block: u64) -> bool {
    index
        .entries
        .binary_search_by(|entry| {
            entry
                .namespace
                .as_str()
                .cmp(namespace)
                .then_with(|| entry.key.as_str().cmp(key))
        })
        .ok()
        .and_then(|position| index.entries.get(position))
        .is_some_and(|entry| entry.min_block <= block && index.start_block <= block)
}

#[derive(Clone)]
struct RpcState {
    publication: Arc<Publication>,
    batch_limit: usize,
}

#[derive(Deserialize)]
struct Request {
    #[serde(default)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

pub async fn serve(
    publication: Arc<Publication>,
    listen: SocketAddr,
    batch_limit: usize,
) -> Result<()> {
    let state = RpcState {
        publication,
        batch_limit,
    };
    let app = Router::new()
        .route("/", post(rpc_handler))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "RPC server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<RpcState>) -> impl IntoResponse {
    Json(json!({
        "status": "ready",
        "generation": state.publication.manifest.generation,
        "published": quantity(state.publication.manifest.published_number),
        "manifest": state.publication.manifest_digest,
    }))
}

async fn metrics(State(state): State<RpcState>) -> impl IntoResponse {
    (StatusCode::OK, state.publication.metrics())
}

async fn rpc_handler(State(state): State<RpcState>, Json(value): Json<Value>) -> impl IntoResponse {
    Json(handle_rpc(state.publication.as_ref(), value, state.batch_limit).await)
}

pub async fn handle_rpc(publication: &Publication, value: Value, batch_limit: usize) -> Value {
    if let Value::Array(requests) = value {
        if requests.is_empty() || requests.len() > batch_limit {
            return error(Value::Null, -32600, "invalid batch size");
        }
        let mut responses = Vec::with_capacity(requests.len());
        for value in requests {
            responses.push(handle_value(publication, value).await);
        }
        Value::Array(responses)
    } else {
        handle_value(publication, value).await
    }
}

async fn handle_value(publication: &Publication, value: Value) -> Value {
    let request = match serde_json::from_value::<Request>(value) {
        Ok(request) if request.jsonrpc.as_deref() == Some("2.0") => request,
        _ => return error(Value::Null, -32600, "invalid request"),
    };
    let id = request.id.clone();
    match dispatch(publication, &request).await {
        Ok(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
        Err(RpcError { code, message }) => error(id, code, &message),
    }
}

struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid(error: impl std::fmt::Display) -> Self {
        Self {
            code: -32602,
            message: error.to_string(),
        }
    }

    fn selector(error: impl std::fmt::Display) -> Self {
        let message = error.to_string();
        Self {
            code: if message.contains("block unavailable") {
                -32001
            } else {
                -32602
            },
            message,
        }
    }

    fn state(error: impl std::fmt::Display) -> Self {
        let message = error.to_string();
        Self {
            code: if message.contains("block unavailable") {
                -32001
            } else {
                -32603
            },
            message,
        }
    }
}

async fn dispatch(publication: &Publication, request: &Request) -> Result<Value, RpcError> {
    match request.method.as_str() {
        "web3_clientVersion" => Ok(json!(format!("fossil/{}", env!("CARGO_PKG_VERSION")))),
        "eth_chainId" => Ok(json!(quantity(publication.manifest.chain_id))),
        "eth_blockNumber" => Ok(json!(quantity(publication.manifest.published_number))),
        "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => {
            let params = params(&request.params, 2)?;
            let address = parse_address(&params[0])?;
            let block = publication
                .resolve_selector(&params[1])
                .map_err(RpcError::selector)?;
            let account = publication
                .account_at(address, block)
                .await
                .map_err(RpcError::state)?;
            match request.method.as_str() {
                "eth_getBalance" => Ok(json!(account
                    .map(|account| quantity_u256(&account.balance))
                    .unwrap_or_else(|| "0x0".to_owned()))),
                "eth_getTransactionCount" => Ok(json!(account
                    .map(|account| quantity(account.nonce))
                    .unwrap_or_else(|| "0x0".to_owned()))),
                "eth_getCode" => {
                    let bytes = publication
                        .code_at(address, block)
                        .await
                        .map_err(RpcError::state)?;
                    Ok(json!(format!("0x{}", hex::encode(bytes))))
                }
                _ => unreachable!(),
            }
        }
        "eth_getStorageAt" => {
            let params = params(&request.params, 3)?;
            let address = parse_address(&params[0])?;
            let slot = parse_slot(&params[1])?;
            let block = publication
                .resolve_selector(&params[2])
                .map_err(RpcError::selector)?;
            let value = publication
                .storage_at(address, slot, block)
                .await
                .map_err(RpcError::state)?;
            Ok(json!(data32(&value)))
        }
        _ => Err(RpcError {
            code: -32601,
            message: "method not found".to_owned(),
        }),
    }
}

fn params(value: &Value, count: usize) -> Result<&[Value], RpcError> {
    let values = value
        .as_array()
        .ok_or_else(|| RpcError::invalid("params must be an array"))?;
    if values.len() != count {
        return Err(RpcError::invalid(format!("expected {count} parameters")));
    }
    Ok(values)
}

fn parse_address(value: &Value) -> Result<Address, RpcError> {
    value
        .as_str()
        .ok_or_else(|| RpcError::invalid("address must be a string"))?
        .parse()
        .map_err(RpcError::invalid)
}

fn parse_slot(value: &Value) -> Result<Hash32, RpcError> {
    let value = value
        .as_str()
        .ok_or_else(|| RpcError::invalid("slot must be a string"))?;
    if value.len() == 66 {
        return value.parse().map_err(RpcError::invalid);
    }
    let quantity = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::invalid("slot must be hex"))?;
    if quantity.is_empty() || quantity.len() > 64 {
        return Err(RpcError::invalid("invalid slot"));
    }
    let padded = format!("0x{quantity:0>64}");
    padded.parse().map_err(RpcError::invalid)
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":message}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{IndexEntry, INDEX_SCHEMA};

    #[test]
    fn sorted_index_point_lookup_uses_exact_key() {
        let index = SegmentIndex {
            schema: INDEX_SCHEMA.to_owned(),
            segment: Hash32([1; 32]),
            decoded_sha256: Hash32([2; 32]),
            start_block: 10,
            end_block: 20,
            entries: vec![
                IndexEntry {
                    namespace: "account".to_owned(),
                    key: "a".to_owned(),
                    min_block: 10,
                    max_block: 20,
                },
                IndexEntry {
                    namespace: "account".to_owned(),
                    key: "b".to_owned(),
                    min_block: 12,
                    max_block: 20,
                },
                IndexEntry {
                    namespace: "storage".to_owned(),
                    key: "a".to_owned(),
                    min_block: 10,
                    max_block: 20,
                },
            ],
        };
        assert!(index_has(&index, "account", "b", 12));
        assert!(!index_has(&index, "account", "b", 11));
        assert!(!index_has(&index, "account", "c", 20));
    }
}
