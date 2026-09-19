use crate::archive::{load_head_manifest, publish, PublicationGate};
use crate::format::{canonical_json, Address, Hash32};
use crate::normalized::read_package;
use crate::rpc::Publication;
use crate::store::{ArchiveStore, MemoryArchiveStore, VersionedBytes};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const ACCOUNT_COUNT: usize = 256;
const COLD_QUERIES: usize = 32;
const WARM_QUERIES: usize = 2_000;
const SEED: u64 = 0xf055_11c0_ffee_2026;

#[derive(Clone, Copy)]
struct IoSnapshot {
    gets: u64,
    bytes: u64,
}

struct CountingStore {
    inner: Arc<dyn ArchiveStore>,
    gets: AtomicU64,
    bytes: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<dyn ArchiveStore>) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }

    fn record(&self, bytes: usize) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[async_trait]
impl ArchiveStore for CountingStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let bytes = self.inner.get(key).await?;
        self.record(bytes.len());
        Ok(bytes)
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes).await
    }

    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        let value = self.inner.read_mutable(key).await?;
        self.record(value.as_ref().map_or(0, |entry| entry.bytes.len()));
        Ok(value)
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()> {
        self.inner.compare_and_swap(key, expected, bytes).await
    }
}

pub async fn run(output: Option<&Path>) -> Result<Value> {
    let input = synthetic_package();
    let package = read_package(&input)?;
    let decoded_segment_bytes = canonical_json(&package.segment)?.len() as u64;
    let addresses: Vec<Address> = (0..ACCOUNT_COUNT).map(address).collect();

    let backing: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    let counting = Arc::new(CountingStore::new(backing));
    let store: Arc<dyn ArchiveStore> = counting.clone();
    publish(
        store.clone(),
        package,
        PublicationGate::Finalized {
            number: 0,
            hash: Hash32([1; 32]),
        },
    )
    .await?;
    let (_, _, manifest) = load_head_manifest(store.as_ref(), 1).await?;
    let compressed_bytes: u64 = manifest
        .segments
        .iter()
        .map(|segment| segment.segment_size)
        .sum();
    let segment_and_index_bytes: u64 = manifest
        .segments
        .iter()
        .map(|segment| segment.segment_size + segment.index_size)
        .sum();
    let manifest_bytes = canonical_json(&manifest)?.len() as u64;
    let archive_bytes = segment_and_index_bytes + manifest_bytes;

    let mut rng = XorShift64(SEED);
    let mut cold_latencies = Vec::with_capacity(COLD_QUERIES);
    let mut measured_gets = 0_u64;
    let mut measured_bytes = 0_u64;
    for _ in 0..COLD_QUERIES {
        let cache = tempfile::tempdir()?;
        let publication =
            Publication::load(store.clone(), 1, cache.path().to_path_buf(), 8).await?;
        let before = counting.snapshot();
        let started = Instant::now();
        let account = publication
            .account_at(addresses[rng.index(ACCOUNT_COUNT)], 0)
            .await?;
        std::hint::black_box(account);
        cold_latencies.push(started.elapsed().as_nanos());
        let after = counting.snapshot();
        measured_gets += after.gets - before.gets;
        measured_bytes += after.bytes - before.bytes;
    }

    let warm_cache = tempfile::tempdir()?;
    let publication =
        Publication::load(store.clone(), 1, warm_cache.path().to_path_buf(), 8).await?;
    publication.account_at(addresses[0], 0).await?;
    let mut warm_latencies = Vec::with_capacity(WARM_QUERIES);
    let warm_io_before = counting.snapshot();
    let throughput_started = Instant::now();
    for _ in 0..WARM_QUERIES {
        let started = Instant::now();
        let account = publication
            .account_at(addresses[rng.index(ACCOUNT_COUNT)], 0)
            .await?;
        std::hint::black_box(account);
        warm_latencies.push(started.elapsed().as_nanos());
    }
    let throughput_elapsed = throughput_started.elapsed();
    let warm_io_after = counting.snapshot();
    measured_gets += warm_io_after.gets - warm_io_before.gets;
    measured_bytes += warm_io_after.bytes - warm_io_before.bytes;
    let run_total = counting.snapshot();

    let query_count = (COLD_QUERIES + WARM_QUERIES) as u64;
    let gets_per_query = measured_gets as f64 / query_count as f64;
    let bytes_per_query = measured_bytes as f64 / query_count as f64;
    let returned_bytes = query_count * 32;
    let read_amplification = measured_bytes as f64 / returned_bytes as f64;
    let archive_gb = archive_bytes as f64 / 1_000_000_000_f64;
    let result = json!({
        "schema": "fossil-benchmark/1",
        "claim": "local synthetic prototype measurement; not a production performance or cost claim",
        "reproduction_command": "cargo run --release -- benchmark --output benchmarks/results/2026-09-19-local.json",
        "dataset": {
            "seed": format!("0x{SEED:016x}"),
            "blocks": 1,
            "accounts": ACCOUNT_COUNT,
            "account_events": ACCOUNT_COUNT,
            "storage_events": 0,
            "code_blobs": 0,
            "cold_queries": COLD_QUERIES,
            "warm_queries": WARM_QUERIES,
            "query": "account point lookup at the published block",
            "cold_definition": "verified indexes loaded before timer; segment absent from memory and disk caches",
            "warm_definition": "segment primed in weighted memory cache before timer"
        },
        "size": {
            "input_jsonl_bytes": input.len(),
            "decoded_segment_bytes": decoded_segment_bytes,
            "compressed_segment_bytes": compressed_bytes,
            "compression_ratio_decoded_over_compressed": decoded_segment_bytes as f64 / compressed_bytes as f64,
            "segment_count": manifest.segments.len(),
            "archive_segment_and_index_bytes": segment_and_index_bytes,
            "manifest_bytes": manifest_bytes,
            "total_immutable_archive_bytes": archive_bytes
        },
        "latency_microseconds": {
            "cold": percentiles(&mut cold_latencies),
            "warm": percentiles(&mut warm_latencies)
        },
        "throughput": {
            "concurrency": 1,
            "warm_queries": WARM_QUERIES,
            "elapsed_seconds": throughput_elapsed.as_secs_f64(),
            "queries_per_second": WARM_QUERIES as f64 / throughput_elapsed.as_secs_f64()
        },
        "backend_io": {
            "measured_query_gets": measured_gets,
            "measured_query_bytes": measured_bytes,
            "gets_per_query": gets_per_query,
            "bytes_per_query": bytes_per_query,
            "read_amplification_over_32_byte_value": read_amplification,
            "cold_gets_per_query": measured_gets as f64 / COLD_QUERIES as f64,
            "cold_bytes_per_query": measured_bytes as f64 / COLD_QUERIES as f64,
            "cold_read_amplification_over_32_byte_value": measured_bytes as f64 / (COLD_QUERIES * 32) as f64,
            "whole_run_gets_including_publication_and_index_startup": run_total.gets,
            "whole_run_bytes_including_publication_and_index_startup": run_total.bytes,
            "cache_hit_ratio": WARM_QUERIES as f64 / query_count as f64
        },
        "build": {
            "fossil_version": env!("CARGO_PKG_VERSION"),
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH
        },
        "cost_assumptions": {
            "as_of": "2026-09-19",
            "provider": "Cloudflare R2 Standard",
            "currency": "USD",
            "decimal_gb": true,
            "assumptions_to_recheck": {
                "storage_per_gb_month": 0.015,
                "class_b_reads_per_million": 0.36,
                "internet_egress_per_gb": 0.0
            },
            "formulas": {
                "measured_archive_storage_per_month": "total_immutable_archive_bytes / 1e9 * storage_per_gb_month",
                "measured_get_cost_per_million_queries": "gets_per_query * class_b_reads_per_million",
                "excludes": "minimum billing, writes, cache infrastructure, CPU, operations, and provider pricing changes"
            },
            "illustrative_results_not_production_claims": {
                "archive_storage_per_month": archive_gb * 0.015,
                "get_cost_per_million_queries": gets_per_query * 0.36
            }
        }
    });

    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(&result)?)
            .with_context(|| format!("write benchmark result {}", path.display()))?;
    }
    Ok(result)
}

fn address(index: usize) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[18..].copy_from_slice(&(index as u16).to_be_bytes());
    Address(bytes)
}

fn synthetic_package() -> Vec<u8> {
    let hash = Hash32([1; 32]);
    let empty_code = Hash32(Keccak256::digest([]).into());
    let mut lines = vec![
        format!(
            "{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{hash}\",\"mode\":\"anchor\",\"anchor_number\":\"0x0\",\"anchor_hash\":\"{hash}\"}}"
        ),
        format!(
            "{{\"type\":\"block\",\"number\":\"0x0\",\"hash\":\"{hash}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x0\"}}",
            Hash32([0; 32]),
            Hash32([2; 32])
        ),
    ];
    for index in 0..ACCOUNT_COUNT {
        lines.push(format!(
            "{{\"type\":\"account\",\"block\":\"0x0\",\"address\":\"{}\",\"exists\":true,\"incarnation\":\"0x0\",\"nonce\":\"0x0\",\"balance\":\"0x{:x}\",\"code_hash\":\"{empty_code}\"}}",
            address(index),
            index + 1
        ));
    }
    lines.push(format!(
        "{{\"type\":\"block_end\",\"number\":\"0x0\",\"event_count\":\"0x{:x}\"}}",
        ACCOUNT_COUNT
    ));
    lines.push(format!(
        "{{\"type\":\"trailer\",\"end_number\":\"0x0\",\"end_hash\":\"{hash}\",\"block_count\":\"0x1\"}}"
    ));
    format!("{}\n", lines.join("\n")).into_bytes()
}

fn percentiles(values: &mut [u128]) -> Value {
    values.sort_unstable();
    json!({
        "p50": percentile(values, 50) as f64 / 1_000.0,
        "p95": percentile(values, 95) as f64 / 1_000.0,
        "p99": percentile(values, 99) as f64 / 1_000.0
    })
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    let index = (values.len() * percentile).div_ceil(100).saturating_sub(1);
    values[index]
}

struct XorShift64(u64);

impl XorShift64 {
    fn index(&mut self, upper: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize % upper
    }
}
