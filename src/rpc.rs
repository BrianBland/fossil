use crate::archive;
use crate::format::{
    data32, parse_quantity, quantity, quantity_u256, AccountEvent, Address, Hash32,
};
use crate::store::ArchiveStore;
use crate::tiered;
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const BATCH_CONCURRENCY: usize = 8;

#[async_trait]
pub trait RpcBackend: Send + Sync {
    fn chain_id(&self) -> u64;
    fn generation(&self) -> u64;
    fn anchor_number(&self) -> u64;
    fn published_number(&self) -> u64;
    fn publication_digest(&self) -> Hash32;
    fn finalized(&self) -> bool;
    fn supports_block_hash_selector(&self) -> bool;
    fn metrics(&self) -> String;
    async fn account_at(&self, address: Address, block: u64) -> Result<Option<AccountEvent>>;
    async fn storage_at(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]>;
    async fn code_at(&self, address: Address, block: u64) -> Result<Vec<u8>>;
    async fn block_number_by_hash(&self, hash: Hash32) -> Result<Option<u64>>;
}

#[async_trait]
impl RpcBackend for archive::Publication {
    fn chain_id(&self) -> u64 {
        self.commit.chain_id
    }
    fn generation(&self) -> u64 {
        self.commit.generation
    }
    fn anchor_number(&self) -> u64 {
        self.commit.anchor_number
    }
    fn published_number(&self) -> u64 {
        self.commit.published_number
    }
    fn publication_digest(&self) -> Hash32 {
        self.head.commit.digest
    }
    fn finalized(&self) -> bool {
        self.commit.finalized
    }
    fn supports_block_hash_selector(&self) -> bool {
        false
    }
    fn metrics(&self) -> String {
        format!("fossil_publication_generation {}\nfossil_published_block {}\nfossil_archive_format 1\n", self.commit.generation, self.commit.published_number)
    }
    async fn account_at(&self, address: Address, block: u64) -> Result<Option<AccountEvent>> {
        archive::Publication::account_at(self, address, block).await
    }
    async fn storage_at(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]> {
        archive::Publication::storage_at(self, address, slot, block).await
    }
    async fn code_at(&self, address: Address, block: u64) -> Result<Vec<u8>> {
        archive::Publication::code_at(self, address, block).await
    }
    async fn block_number_by_hash(&self, hash: Hash32) -> Result<Option<u64>> {
        archive::Publication::block_number_by_hash(self, hash).await
    }
}

#[async_trait]
impl RpcBackend for tiered::Reader<'static> {
    fn chain_id(&self) -> u64 {
        tiered::Reader::chain_id(self)
    }
    fn generation(&self) -> u64 {
        self.latest_block()
    }
    fn anchor_number(&self) -> u64 {
        0
    }
    fn published_number(&self) -> u64 {
        self.latest_block()
    }
    fn publication_digest(&self) -> Hash32 {
        self.head_digest()
    }
    fn finalized(&self) -> bool {
        true
    }
    fn supports_block_hash_selector(&self) -> bool {
        false
    }
    fn metrics(&self) -> String {
        format!(
            "fossil_published_block {}\nfossil_runs {}\n",
            self.latest_block(),
            self.run_count()
        )
    }
    async fn account_at(&self, address: Address, block: u64) -> Result<Option<AccountEvent>> {
        self.account(address, block).await
    }
    async fn storage_at(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]> {
        self.storage(address, slot, block).await
    }
    async fn code_at(&self, address: Address, block: u64) -> Result<Vec<u8>> {
        self.code(address, block).await
    }
    async fn block_number_by_hash(&self, _hash: Hash32) -> Result<Option<u64>> {
        Ok(None)
    }
}

/// Serve the tiered archive, adopting each newer verified head (including
/// same-block compactions) at `interval`. Readers pin one head per request.
pub async fn serve_tiered(
    store: &'static dyn ArchiveStore,
    chain_id: u64,
    interval: Option<Duration>,
    listen: SocketAddr,
    batch_limit: usize,
) -> Result<()> {
    let current: Arc<dyn RpcBackend> = Arc::new(tiered::Reader::open(store, chain_id).await?);
    let Some(interval) = interval.filter(|interval| !interval.is_zero()) else {
        return serve_backend(current, listen, batch_limit).await;
    };
    let shared = Arc::new(RwLock::new(current));
    let target = shared.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match tiered::Reader::open(store, chain_id).await {
                Ok(candidate) => {
                    let mut current = target.write().expect("publication lock poisoned");
                    if candidate.latest_block() < current.published_number() {
                        tracing::warn!("refresh would roll back; retaining previous head");
                    } else if candidate.head_digest() != current.publication_digest() {
                        *current = Arc::new(candidate);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "refresh rejected; retaining previous head")
                }
            }
        }
    });
    serve_state(
        RpcState {
            publication: PublicationSource::Refreshing(shared),
            batch_limit,
        },
        listen,
    )
    .await
}

/// Answer one JSON-RPC value (or batch) against any backend.
pub async fn handle_rpc_with(backend: &dyn RpcBackend, value: Value, batch_limit: usize) -> Value {
    handle_rpc_backend(backend, value, batch_limit).await
}

#[derive(Clone)]
enum PublicationSource {
    Static(Arc<dyn RpcBackend>),
    Refreshing(Arc<RwLock<Arc<dyn RpcBackend>>>),
}

#[derive(Clone)]
struct RpcState {
    publication: PublicationSource,
    batch_limit: usize,
}

impl RpcState {
    fn snapshot(&self) -> Arc<dyn RpcBackend> {
        match &self.publication {
            PublicationSource::Static(publication) => publication.clone(),
            PublicationSource::Refreshing(publication) => publication
                .read()
                .expect("publication lock poisoned")
                .clone(),
        }
    }
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
    publication: Arc<archive::Publication>,
    listen: SocketAddr,
    batch_limit: usize,
) -> Result<()> {
    serve_backend(publication, listen, batch_limit).await
}

fn validate_refresh(
    current: &archive::Publication,
    candidate: &archive::Publication,
) -> Result<bool> {
    if candidate.head.commit == current.head.commit {
        return Ok(false);
    }
    if candidate.commit.generation <= current.commit.generation
        || candidate.commit.published_number <= current.commit.published_number
    {
        bail!("refresh would roll back or fail to advance the publication");
    }
    if candidate.commit.chain_id != current.commit.chain_id
        || candidate.commit.genesis_hash != current.commit.genesis_hash
        || candidate.commit.anchor_number != current.commit.anchor_number
        || candidate.commit.anchor_hash != current.commit.anchor_hash
    {
        bail!("refresh publication identity is unrelated to the current chain");
    }
    // The authoritative mutable head may advance more than once between polls. Its
    // audit parent is not traversed by readers, so skipped generations are valid.
    Ok(true)
}

pub async fn serve_refreshing(
    publication: Arc<archive::Publication>,
    store: Arc<dyn ArchiveStore>,
    chain_id: u64,
    interval: Duration,
    listen: SocketAddr,
    batch_limit: usize,
) -> Result<()> {
    if interval.is_zero() {
        bail!("refresh interval must be greater than zero");
    }
    let mut accepted = publication.clone();
    let shared: Arc<RwLock<Arc<dyn RpcBackend>>> = Arc::new(RwLock::new(publication));
    let refresh_target = shared.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match archive::Publication::load(store.clone(), chain_id).await {
                Ok(candidate) => match validate_refresh(accepted.as_ref(), &candidate) {
                    Ok(true) => {
                        accepted = Arc::new(candidate);
                        *refresh_target.write().expect("publication lock poisoned") =
                            accepted.clone();
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, "refresh rejected; retaining previous publication")
                    }
                },
                Err(error) => {
                    tracing::warn!(%error, "refresh rejected; retaining previous publication")
                }
            }
        }
    });
    serve_state(
        RpcState {
            publication: PublicationSource::Refreshing(shared),
            batch_limit,
        },
        listen,
    )
    .await
}

async fn serve_backend(
    publication: Arc<dyn RpcBackend>,
    listen: SocketAddr,
    batch_limit: usize,
) -> Result<()> {
    let state = RpcState {
        publication: PublicationSource::Static(publication),
        batch_limit,
    };
    serve_state(state, listen).await
}

async fn serve_state(state: RpcState, listen: SocketAddr) -> Result<()> {
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
    let publication = state.snapshot();
    Json(json!({
        "status": "ready",
        "generation": publication.generation(),
        "published": quantity(publication.published_number()),
        "manifest": publication.publication_digest(),
    }))
}

async fn metrics(State(state): State<RpcState>) -> impl IntoResponse {
    (StatusCode::OK, state.snapshot().metrics())
}

async fn rpc_handler(State(state): State<RpcState>, Json(value): Json<Value>) -> impl IntoResponse {
    let publication = state.snapshot();
    Json(handle_rpc_backend(publication.as_ref(), value, state.batch_limit).await)
}

pub async fn handle_rpc(
    publication: &archive::Publication,
    value: Value,
    batch_limit: usize,
) -> Value {
    handle_rpc_backend(publication, value, batch_limit).await
}

async fn handle_rpc_backend(
    publication: &dyn RpcBackend,
    value: Value,
    batch_limit: usize,
) -> Value {
    if let Value::Array(requests) = value {
        if requests.is_empty() || requests.len() > batch_limit {
            return error(Value::Null, -32600, "invalid batch size");
        }
        let responses = stream::iter(requests)
            .map(|value| handle_value(publication, value))
            .buffered(BATCH_CONCURRENCY)
            .collect()
            .await;
        Value::Array(responses)
    } else {
        handle_value(publication, value).await
    }
}

async fn handle_value(publication: &dyn RpcBackend, value: Value) -> Value {
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

async fn resolve_selector(publication: &dyn RpcBackend, selector: &Value) -> Result<u64> {
    let selector = match selector {
        Value::Object(object) => {
            if let Some(number) = object.get("blockNumber") {
                if object.contains_key("blockHash") {
                    bail!("selector cannot contain blockNumber and blockHash");
                }
                number
            } else {
                if !publication.supports_block_hash_selector() {
                    bail!("block-hash selectors are unsupported until a bounded durable hash index exists");
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
                return publication
                    .block_number_by_hash(hash)
                    .await?
                    .ok_or_else(|| anyhow!("block unavailable"));
            }
        }
        value => value,
    };
    let number = match selector {
        Value::String(tag) => match tag.as_str() {
            "latest" => publication.published_number(),
            "safe" | "finalized" if !publication.finalized() => {
                bail!("block unavailable: fixed-offset publication is not safe or finalized")
            }
            "safe" | "finalized" => publication.published_number(),
            "earliest" if publication.anchor_number() == 0 => 0,
            "earliest" => bail!("block unavailable"),
            "pending" => bail!("pending is unsupported"),
            value => parse_quantity(value)?,
        },
        _ => bail!("invalid block selector"),
    };
    if number < publication.anchor_number() || number > publication.published_number() {
        bail!("block unavailable");
    }
    Ok(number)
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

async fn dispatch(publication: &dyn RpcBackend, request: &Request) -> Result<Value, RpcError> {
    match request.method.as_str() {
        "web3_clientVersion" => Ok(json!(format!("fossil/{}", env!("CARGO_PKG_VERSION")))),
        "eth_chainId" => Ok(json!(quantity(publication.chain_id()))),
        "eth_blockNumber" => Ok(json!(quantity(publication.published_number()))),
        "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => {
            let params = params(&request.params, 2)?;
            let address = parse_address(&params[0])?;
            let block = resolve_selector(publication, &params[1])
                .await
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
            let block = resolve_selector(publication, &params[2])
                .await
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
    use crate::archive::PublicationGate;
    use crate::normalized::read_package;
    use crate::store::{ArchiveStore, MemoryArchiveStore};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ConcurrentBackend {
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl ConcurrentBackend {
        fn new() -> Self {
            Self {
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            }
        }

        async fn enter(&self) {
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl RpcBackend for ConcurrentBackend {
        fn chain_id(&self) -> u64 {
            1
        }
        fn generation(&self) -> u64 {
            1
        }
        fn anchor_number(&self) -> u64 {
            0
        }
        fn published_number(&self) -> u64 {
            0
        }
        fn publication_digest(&self) -> Hash32 {
            Hash32([1; 32])
        }
        fn finalized(&self) -> bool {
            true
        }
        fn supports_block_hash_selector(&self) -> bool {
            false
        }
        fn metrics(&self) -> String {
            String::new()
        }
        async fn account_at(&self, _address: Address, _block: u64) -> Result<Option<AccountEvent>> {
            self.enter().await;
            Ok(None)
        }
        async fn storage_at(
            &self,
            _address: Address,
            _slot: Hash32,
            _block: u64,
        ) -> Result<[u8; 32]> {
            unreachable!()
        }
        async fn code_at(&self, _address: Address, _block: u64) -> Result<Vec<u8>> {
            unreachable!()
        }
        async fn block_number_by_hash(&self, _hash: Hash32) -> Result<Option<u64>> {
            unreachable!()
        }
    }

    fn package(mode: &str, block: u8) -> Vec<u8> {
        let hash = Hash32([block + 1; 32]);
        let genesis = Hash32([1; 32]);
        let header = if mode == "anchor" {
            format!("{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{genesis}\",\"mode\":\"anchor\",\"anchor_number\":\"0x0\",\"anchor_hash\":\"{hash}\"}}")
        } else {
            let previous = Hash32([block; 32]);
            format!("{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{genesis}\",\"mode\":\"delta\",\"preceding_number\":\"0x{:x}\",\"preceding_hash\":\"{previous}\"}}", block - 1)
        };
        let parent = if block == 0 {
            Hash32([0; 32])
        } else {
            Hash32([block; 32])
        };
        format!(
            "{header}\n{{\"type\":\"block\",\"number\":\"0x{block:x}\",\"hash\":\"{hash}\",\"parent_hash\":\"{parent}\",\"state_root\":\"{}\",\"timestamp\":\"0x{block:x}\"}}\n{{\"type\":\"block_end\",\"number\":\"0x{block:x}\",\"event_count\":\"0x0\"}}\n{{\"type\":\"trailer\",\"end_number\":\"0x{block:x}\",\"end_hash\":\"{hash}\",\"block_count\":\"0x1\"}}\n",
            Hash32([100 + block; 32])
        ).into_bytes()
    }

    #[tokio::test]
    async fn batch_runs_at_most_eight_entries_concurrently_and_preserves_order() {
        let backend = ConcurrentBackend::new();
        let requests = (0..12)
            .map(|id| {
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "eth_getBalance",
                    "params": ["0x0000000000000000000000000000000000000001", "latest"]
                })
            })
            .collect();
        let response = handle_rpc_backend(&backend, Value::Array(requests), 100).await;
        let ids: Vec<_> = response
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["id"].as_u64().unwrap())
            .collect();
        assert_eq!(ids, (0..12).collect::<Vec<_>>());
        assert_eq!(backend.max_in_flight.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn refresh_ignores_equal_accepts_skips_and_rejects_rollback_or_unrelated() {
        let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        archive::publish(
            store.clone(),
            read_package(&package("anchor", 0)).unwrap(),
            PublicationGate::Finalized {
                number: 0,
                hash: Hash32([1; 32]),
            },
        )
        .await
        .unwrap();
        let first = archive::Publication::load(store.clone(), 1).await.unwrap();
        assert!(!validate_refresh(&first, &first).unwrap());

        archive::publish(
            store.clone(),
            read_package(&package("delta", 1)).unwrap(),
            PublicationGate::Finalized {
                number: 1,
                hash: Hash32([2; 32]),
            },
        )
        .await
        .unwrap();
        let second = archive::Publication::load(store, 1).await.unwrap();
        assert!(validate_refresh(&first, &second).unwrap());
        assert!(validate_refresh(&second, &first).is_err());

        let mut unrelated = second.clone();
        unrelated.commit.genesis_hash = Hash32([99; 32]);
        assert!(validate_refresh(&first, &unrelated).is_err());
        let mut skipped = second.clone();
        skipped.commit.generation = 3;
        skipped.commit.published_number = 2;
        skipped.head.generation = 3;
        skipped.head.number = 2;
        skipped.commit.parent = archive::ObjectRef {
            digest: Hash32([88; 32]),
            length: 1,
        };
        assert!(validate_refresh(&first, &skipped).unwrap());
    }
}
