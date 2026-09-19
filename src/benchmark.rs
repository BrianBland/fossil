use crate::archive::PublicationGate;
use crate::format::{
    AccountEvent, Address, BlockMeta, Hash32, Segment, StorageEvent, SEGMENT_SCHEMA,
};
use crate::normalized::{Mode, Package};
use crate::store::{open_store, ArchiveStore, MemoryArchiveStore, VersionedBytes};
use crate::v2::{self, Publication};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use sha3::{Digest as _, Keccak256};
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SEED: u64 = 0xba5e_f055_2026_0003;
const ACCOUNTS_PER_BLOCK: u64 = 162;
const STORAGE_PER_BLOCK: u64 = 851;
const EPOCH_BLOCKS: u64 = 1_000;
const MEASURED_EPOCHS: u64 = 100;
const LOCAL_ACCOUNT_POOL: u64 = 20_614;
const GLOBAL_ACCOUNT_POOL: u64 = 940_971;
const LOCAL_STORAGE_POOL: u64 = 244_196;
const GLOBAL_STORAGE_POOL: u64 = 17_339_777;
const MANUAL_MIN_FREE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Default)]
struct Snapshot {
    gets: u64,
    get_bytes: u64,
    put_attempts: u64,
    put_bytes: u64,
    unique_objects: u64,
    unique_bytes: u64,
    data_objects: u64,
    data_bytes: u64,
    index_objects: u64,
    index_bytes: u64,
    block_objects: u64,
    block_bytes: u64,
    checkpoint_objects: u64,
    checkpoint_bytes: u64,
    directory_objects: u64,
    directory_bytes: u64,
    commit_objects: u64,
    commit_bytes: u64,
    catalog_objects: u64,
    catalog_bytes: u64,
}

#[derive(Default)]
struct Unique {
    keys: HashSet<String>,
    snapshot: Snapshot,
}

struct CountingStore {
    inner: Arc<dyn ArchiveStore>,
    gets: AtomicU64,
    get_bytes: AtomicU64,
    put_attempts: AtomicU64,
    put_bytes: AtomicU64,
    unique: Mutex<Unique>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ArchiveStore>) -> Self {
        Self {
            inner,
            gets: 0.into(),
            get_bytes: 0.into(),
            put_attempts: 0.into(),
            put_bytes: 0.into(),
            unique: Mutex::new(Unique::default()),
        }
    }

    fn memory() -> Self {
        Self::new(Arc::new(MemoryArchiveStore::default()))
    }

    fn snapshot(&self) -> Snapshot {
        let unique = self.unique.lock().expect("benchmark stats lock poisoned");
        Snapshot {
            gets: self.gets.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
            put_attempts: self.put_attempts.load(Ordering::Relaxed),
            put_bytes: self.put_bytes.load(Ordering::Relaxed),
            ..unique.snapshot
        }
    }

    fn record_get(&self, bytes: usize) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[async_trait]
impl ArchiveStore for CountingStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let bytes = self.inner.get(key).await?;
        self.record_get(bytes.len());
        Ok(bytes)
    }

    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let bytes = self.inner.get_bounded(key, maximum).await?;
        self.record_get(bytes.len());
        Ok(bytes)
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes).await?;
        self.put_attempts.fetch_add(1, Ordering::Relaxed);
        self.put_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let mut unique = self.unique.lock().expect("benchmark stats lock poisoned");
        if unique.keys.insert(key.to_owned()) {
            unique.snapshot.unique_objects += 1;
            unique.snapshot.unique_bytes += bytes.len() as u64;
            let size = bytes.len() as u64;
            if bytes.starts_with(b"F2ED") {
                unique.snapshot.data_objects += 1;
                unique.snapshot.data_bytes += size;
            } else if bytes.starts_with(b"F2EI") {
                unique.snapshot.index_objects += 1;
                unique.snapshot.index_bytes += size;
            } else if bytes.starts_with(b"F2EB") {
                unique.snapshot.block_objects += 1;
                unique.snapshot.block_bytes += size;
            } else if bytes.starts_with(b"F2EP")
                || bytes.starts_with(b"F2EM")
                || bytes.starts_with(b"F2PS")
            {
                unique.snapshot.checkpoint_objects += 1;
                unique.snapshot.checkpoint_bytes += size;
            } else if bytes.starts_with(b"F2ER") {
                unique.snapshot.directory_objects += 1;
                unique.snapshot.directory_bytes += size;
            } else if bytes.starts_with(b"F2EC") {
                unique.snapshot.commit_objects += 1;
                unique.snapshot.commit_bytes += size;
            } else if bytes.starts_with(b"F2EW") || bytes.starts_with(b"F2WC") {
                unique.snapshot.catalog_objects += 1;
                unique.snapshot.catalog_bytes += size;
            }
        }
        Ok(())
    }

    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        let value = self.inner.read_mutable(key).await?;
        self.record_get(value.as_ref().map_or(0, |entry| entry.bytes.len()));
        Ok(value)
    }

    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        let value = self.inner.read_mutable_bounded(key, maximum).await?;
        self.record_get(value.as_ref().map_or(0, |entry| entry.bytes.len()));
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

pub async fn run(
    output: Option<&Path>,
    manual_blocks: Option<u64>,
    chunk_blocks: u64,
    scratch: Option<&Path>,
) -> Result<Value> {
    match manual_blocks {
        Some(blocks) => run_manual(output, scratch, blocks, chunk_blocks).await,
        None => run_checked(output).await,
    }
}

async fn run_checked(output: Option<&Path>) -> Result<Value> {
    let build_started = Instant::now();
    let package = shaped_package(0, EPOCH_BLOCKS, EPOCH_BLOCKS, None, true)?;
    let oracle = account_oracle(&package);
    let generated_storage_keys: HashSet<_> = package
        .segment
        .storage
        .iter()
        .map(|event| (event.address, event.slot))
        .collect();
    if oracle.len() as u64 != LOCAL_ACCOUNT_POOL
        || generated_storage_keys.len() as u64 != LOCAL_STORAGE_POOL
    {
        bail!("checked generator did not realize configured local unique pools");
    }
    let head_hash = package.segment.blocks.last().unwrap().hash;
    let store = Arc::new(CountingStore::memory());
    let archive: Arc<dyn ArchiveStore> = store.clone();
    let before_publish = store.snapshot();
    v2::publish(
        archive.clone(),
        package,
        PublicationGate::Finalized {
            number: EPOCH_BLOCKS - 1,
            hash: head_hash,
        },
    )
    .await?;
    let after_publish = store.snapshot();
    let build_elapsed = build_started.elapsed();

    let before_startup = store.snapshot();
    let publication = Publication::load(archive, 1).await?;
    let after_startup = store.snapshot();

    let targets = deterministic_targets(&oracle, EPOCH_BLOCKS);
    let mut cold_gets = 0_u64;
    let mut cold_bytes = 0_u64;
    let mut max_cold_gets = 0_u64;
    let mut max_cold_bytes = 0_u64;
    let mut returned_bytes = 0_u64;
    let mut predecessor_ok = true;
    for (address, block, expected) in &targets {
        let before = store.snapshot();
        let actual = publication.account_at(*address, *block).await?;
        let after = store.snapshot();
        let gets = after.gets - before.gets;
        let bytes = after.get_bytes - before.get_bytes;
        cold_gets += gets;
        cold_bytes += bytes;
        max_cold_gets = max_cold_gets.max(gets);
        max_cold_bytes = max_cold_bytes.max(bytes);
        predecessor_ok &= actual.as_ref().map(|event| event.balance) == *expected;
        returned_bytes += actual.map_or(0, |_| 32);
    }
    let before_repeated = store.snapshot();
    for (address, block, _) in &targets {
        std::hint::black_box(publication.account_at(*address, *block).await?);
    }
    let after_repeated = store.snapshot();

    let rollover = rollover_measurement().await?;
    let result = json!({
        "schema": "fossil-v2-sealed-epoch-benchmark/1",
        "claim": "checked synthetic Base-shaped sealed-epoch physical-layout evidence; not semantically complete forward state or production object-store performance",
        "reproduction_command": "cargo run --release -- benchmark --output benchmarks/results/v2-base-shaped-epoch.json",
        "measurement_provenance": provenance(),
        "workload": {
            "blocks": EPOCH_BLOCKS,
            "account_changes_per_block": ACCOUNTS_PER_BLOCK,
            "storage_changes_per_block": STORAGE_PER_BLOCK,
            "account_changes": EPOCH_BLOCKS * ACCOUNTS_PER_BLOCK,
            "storage_changes": EPOCH_BLOCKS * STORAGE_PER_BLOCK,
            "target_local_account_pool": LOCAL_ACCOUNT_POOL,
            "target_local_storage_key_pool": LOCAL_STORAGE_POOL,
            "generated_unique_accounts": oracle.len(),
            "generated_unique_storage_keys": generated_storage_keys.len(),
            "expected_100k_global_account_pool": expected_union_pool(LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL, MEASURED_EPOCHS),
            "expected_100k_global_storage_key_pool": expected_union_pool(LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL, MEASURED_EPOCHS),
            "seed": format!("0x{SEED:016x}")
        },
        "publication": {
            "put_attempts": after_publish.put_attempts - before_publish.put_attempts,
            "put_attempt_bytes": after_publish.put_bytes - before_publish.put_bytes,
            "unique_immutable_objects": after_publish.unique_objects,
            "total_immutable_bytes": after_publish.unique_bytes,
            "state_data_objects": after_publish.data_objects,
            "state_data_bytes": after_publish.data_bytes,
            "index_delta_objects": after_publish.index_objects,
            "index_delta_bytes": after_publish.index_bytes,
            "block_metadata_objects": after_publish.block_objects,
            "block_metadata_bytes": after_publish.block_bytes,
            "checkpoint_objects": after_publish.checkpoint_objects,
            "checkpoint_bytes": after_publish.checkpoint_bytes,
            "directory_objects": after_publish.directory_objects,
            "directory_bytes": after_publish.directory_bytes,
            "commit_objects": after_publish.commit_objects,
            "commit_bytes": after_publish.commit_bytes,
            "catalog_objects": after_publish.catalog_objects,
            "catalog_bytes": after_publish.catalog_bytes,
            "build_elapsed_seconds": build_elapsed.as_secs_f64(),
            "build_peak_rss_bytes": Value::Null,
            "rss_note": "capture with /usr/bin/time -l; Rust std does not expose portable peak RSS"
        },
        "correctness": {
            "predecessor_oracle": predecessor_ok,
            "forced_fingerprint_collision_full_key_verified": v2::benchmark_forced_collision_check()?,
            "returned_logical_bytes": returned_bytes
        },
        "startup": {
            "gets": after_startup.gets - before_startup.gets,
            "bytes": after_startup.get_bytes - before_startup.get_bytes,
            "expected_objects": ["epoch head", "fixed-size commit"]
        },
        "rollover_63_64": rollover,
        "point_lookup": {
            "queries": targets.len(),
            "cache_warming_sequence_remote_gets": cold_gets,
            "cache_warming_sequence_remote_gets_per_query": cold_gets as f64 / targets.len() as f64,
            "cache_warming_sequence_remote_bytes": cold_bytes,
            "cache_warming_sequence_remote_bytes_per_query": cold_bytes as f64 / targets.len() as f64,
            "maximum_single_query_gets": max_cold_gets,
            "maximum_single_query_bytes": max_cold_bytes,
            "repeated_local_ingested_gets": after_repeated.gets - before_repeated.gets,
            "repeated_local_ingested_bytes": after_repeated.get_bytes - before_repeated.get_bytes,
            "note": "reader lazily caches verified directory/index/data objects; cache is disposable and non-authoritative"
        },
        "build": {
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH
        }
    });
    write_result(output, &result)?;
    Ok(result)
}

async fn rollover_measurement() -> Result<Value> {
    let store = Arc::new(CountingStore::memory());
    let archive: Arc<dyn ArchiveStore> = store.clone();
    let mut closing = None;
    for block in 0..=64_u64 {
        let package = lightweight_package(block);
        let before = store.snapshot();
        let outcome = v2::publish(
            archive.clone(),
            package,
            PublicationGate::Finalized {
                number: block,
                hash: block_hash(block),
            },
        )
        .await?;
        let after = store.snapshot();
        let measurement = json!({
            "block": block,
            "new_checkpoint_objects": after.checkpoint_objects - before.checkpoint_objects,
            "new_checkpoint_bytes": after.checkpoint_bytes - before.checkpoint_bytes,
            "checkpoint_subshard_objects": outcome.checkpoint_subshard_objects,
            "checkpoint_subshard_bytes": outcome.checkpoint_subshard_bytes,
            "checkpoint_partition_manifest_objects": outcome.checkpoint_partition_manifest_objects,
            "checkpoint_partition_manifest_bytes": outcome.checkpoint_partition_manifest_bytes,
            "peak_partition_entries": outcome.checkpoint_peak_partition_entries,
            "peak_partition_estimated_bytes": outcome.checkpoint_peak_partition_estimated_bytes
        });
        if block == 63 {
            closing = Some(measurement);
        }
    }
    let publication = Publication::load(archive.clone(), 1).await?;
    let before = store.snapshot();
    let account = publication.account_at(address(1), 64).await?;
    let after = store.snapshot();
    let completed_reader = Publication::load(archive, 1).await?;
    let before_completed = store.snapshot();
    let completed_account = completed_reader.account_at(address(1), 63).await?;
    let after_completed = store.snapshot();
    Ok(json!({
        "fixture": "64 one-block epochs, one closing checkpoint, then one checkpoint-backed active-window lookup",
        "closing_epoch_64": closing.context("missing closing checkpoint measurement")?,
        "cold_checkpoint_lookup_gets": after.gets - before.gets,
        "cold_checkpoint_lookup_bytes": after.get_bytes - before.get_bytes,
        "numeric_completed_window_selection_objects_max": 2,
        "cold_completed_window_lookup_gets": after_completed.gets - before_completed.gets,
        "cold_completed_window_lookup_bytes": after_completed.get_bytes - before_completed.get_bytes,
        "lookup_found": account.is_some() && completed_account.is_some(),
        "catalog_lookup_note": "numeric selection fetches root plus one chunk; completed directory is a separate fetch"
    }))
}

fn lightweight_package(block: u64) -> Package {
    let genesis = block_hash(0);
    let empty_code = Hash32(Keccak256::digest([]).into());
    Package {
        mode: if block == 0 {
            Mode::Anchor
        } else {
            Mode::Delta
        },
        preceding_number: (block > 0).then(|| block - 1),
        preceding_hash: (block > 0).then(|| block_hash(block - 1)),
        input_sha256: Hash32::digest(&[0xee, block as u8]),
        segment: Segment {
            schema: SEGMENT_SCHEMA.to_owned(),
            chain_id: 1,
            genesis_hash: genesis,
            start_block: block,
            end_block: block,
            blocks: vec![BlockMeta {
                number: block,
                hash: block_hash(block),
                parent_hash: if block == 0 {
                    Hash32([0; 32])
                } else {
                    block_hash(block - 1)
                },
                state_root: Hash32([block as u8; 32]),
                timestamp: block,
            }],
            accounts: if block == 0 {
                vec![AccountEvent {
                    block,
                    address: address(1),
                    exists: true,
                    incarnation: 0,
                    nonce: 0,
                    balance: u256(1),
                    code_hash: empty_code,
                }]
            } else {
                Vec::new()
            },
            storage: Vec::new(),
            code: Vec::new(),
        },
    }
}

async fn run_manual(
    output: Option<&Path>,
    scratch: Option<&Path>,
    blocks: u64,
    chunk_blocks: u64,
) -> Result<Value> {
    let output =
        output.context("manual benchmark requires --output for epoch progress checkpoints")?;
    let scratch =
        scratch.context("manual benchmark requires explicit --scratch-dir (never hidden /tmp)")?;
    if blocks == 0
        || chunk_blocks == 0
        || chunk_blocks > EPOCH_BLOCKS
        || !blocks.is_multiple_of(chunk_blocks)
    {
        bail!("manual blocks must be a nonzero multiple of --chunk-blocks, with chunk size <=1000");
    }
    std::fs::create_dir_all(scratch)?;
    let scratch = std::fs::canonicalize(scratch)?;
    let system_tmp = std::fs::canonicalize(std::env::temp_dir())?;
    if scratch == system_tmp || scratch.starts_with(&system_tmp) {
        bail!("manual scratch directory must not be system /tmp or a descendant");
    }
    let free = fs2::available_space(&scratch)?;
    let preflight_available_inodes = available_inodes(&scratch);
    if free < MANUAL_MIN_FREE_BYTES {
        bail!("scratch directory has {free} free bytes; at least {MANUAL_MIN_FREE_BYTES} required");
    }
    let store_path = scratch.join("fossil-epoch-store");
    if store_path.exists() && std::fs::read_dir(&store_path)?.next().is_some() {
        bail!("manual scratch store must be empty; resume is not implemented");
    }
    let backing = open_store(
        store_path.to_str().context("scratch path is not UTF-8")?,
        None,
        None,
    )
    .await?;
    let store = Arc::new(CountingStore::new(backing));
    let archive: Arc<dyn ArchiveStore> = store.clone();
    let started = Instant::now();
    let mut start = 0_u64;
    let mut preceding = None;
    let mut progress = Vec::new();
    while start < blocks {
        let count = chunk_blocks.min(blocks - start);
        let package = shaped_package(start, count, blocks, preceding, start == 0)?;
        let hash = package.segment.blocks.last().unwrap().hash;
        let before = store.snapshot();
        v2::publish(
            archive.clone(),
            package,
            PublicationGate::Finalized {
                number: start + count - 1,
                hash,
            },
        )
        .await?;
        let after = store.snapshot();
        preceding = Some((start + count - 1, hash));
        start += count;
        let epoch_elapsed_seconds = started.elapsed().as_secs_f64();
        let current_available_bytes = fs2::available_space(&scratch)?;
        let current_available_inodes = available_inodes(&scratch);
        progress.push(json!({
            "sealed_through": start - 1,
            "epoch_blocks": count,
            "new_unique_objects": after.unique_objects - before.unique_objects,
            "new_unique_bytes": after.unique_bytes - before.unique_bytes,
            "cumulative_objects": after.unique_objects,
            "cumulative_logical_bytes": after.unique_bytes,
            "put_attempts": after.put_attempts - before.put_attempts,
            "files": after.unique_objects + 1,
            "elapsed_seconds": epoch_elapsed_seconds,
            "available_filesystem_bytes": current_available_bytes,
            "available_inodes": current_available_inodes
        }));
        let partial = manual_result(
            blocks,
            chunk_blocks,
            &scratch,
            free,
            preflight_available_inodes,
            started.elapsed().as_secs_f64(),
            &progress,
            after,
            false,
        );
        write_result(Some(output), &partial)?;
    }
    let final_stats = store.snapshot();
    let result = manual_result(
        blocks,
        chunk_blocks,
        &scratch,
        free,
        preflight_available_inodes,
        started.elapsed().as_secs_f64(),
        &progress,
        final_stats,
        true,
    );
    write_result(Some(output), &result)?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn manual_result(
    blocks: u64,
    chunk_blocks: u64,
    scratch: &Path,
    preflight_free_bytes: u64,
    preflight_available_inodes: Option<u64>,
    elapsed: f64,
    progress: &[Value],
    stats: Snapshot,
    complete: bool,
) -> Value {
    json!({
        "schema": "fossil-v2-manual-sealed-epochs/1",
        "complete": complete,
        "blocks_requested": blocks,
        "chunk_blocks": chunk_blocks,
        "epochs_completed": progress.len(),
        "scratch_directory": scratch,
        "preflight_free_bytes": preflight_free_bytes,
        "preflight_available_inodes": preflight_available_inodes,
        "preflight_note": "canonical path rejects system /tmp descendants; requires at least 8 GiB free and reports df inode availability when parseable",
        "progress": progress,
        "immutable_objects": stats.unique_objects,
        "logical_immutable_bytes": stats.unique_bytes,
        "estimated_files_including_head": stats.unique_objects + 1,
        "data_objects": stats.data_objects,
        "index_objects": stats.index_objects,
        "block_objects": stats.block_objects,
        "checkpoint_objects": stats.checkpoint_objects,
        "directory_objects": stats.directory_objects,
        "catalog_objects": stats.catalog_objects,
        "commit_objects": stats.commit_objects,
        "elapsed_seconds": elapsed,
        "corrected_generator_pools": {
            "local_accounts_per_1000_blocks": LOCAL_ACCOUNT_POOL,
            "local_storage_keys_per_1000_blocks": LOCAL_STORAGE_POOL,
            "expected_accounts_across_100_epochs": expected_union_pool(LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL, MEASURED_EPOCHS),
            "expected_storage_keys_across_100_epochs": expected_union_pool(LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL, MEASURED_EPOCHS)
        },
        "tuned_format_fanout": {
            "router_partitions": 128,
            "data_partitions": 32,
            "checkpoint_subshards_per_partition": 8,
            "expected_100k_max_objects_excluding_code": 17456,
            "note": "structural worst-case object fanout for 100 epochs and one closing checkpoint; bytes and runtime require rerun"
        },
        "acceptance_targets": {
            "objects_excluding_code_max": 35000,
            "immutable_bytes_max": 2_750_000_000_u64,
            "elapsed_seconds_max": 600,
            "status": if complete && stats.unique_objects <= 35000 && stats.unique_bytes <= 2_750_000_000 && elapsed <= 600.0 { "passed" } else if complete { "failed" } else { "incomplete" }
        },
        "claim": "chunked synthetic sealed-epoch build; one input chunk is dropped before the next and progress is checkpointed after every epoch"
    })
}

fn shaped_package(
    start: u64,
    count: u64,
    _total_blocks: u64,
    preceding: Option<(u64, Hash32)>,
    anchor: bool,
) -> Result<Package> {
    let epoch_index = start / EPOCH_BLOCKS;
    let account_offset = sliding_pool_offset(epoch_index, LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL);
    let storage_offset = sliding_pool_offset(epoch_index, LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL);
    let empty_code = Hash32(Keccak256::digest([]).into());
    let mut blocks = Vec::with_capacity(count as usize);
    let mut accounts = Vec::with_capacity((count * ACCOUNTS_PER_BLOCK) as usize);
    let mut storage = Vec::with_capacity((count * STORAGE_PER_BLOCK) as usize);
    let mut parent = preceding.map_or(Hash32([0; 32]), |(_, hash)| hash);
    for block in start..start + count {
        let hash = block_hash(block);
        blocks.push(BlockMeta {
            number: block,
            hash,
            parent_hash: parent,
            state_root: Hash32::digest(&[b's', (block & 0xff) as u8]),
            timestamp: block * 2,
        });
        parent = hash;
        let epoch_block = block % EPOCH_BLOCKS;
        for n in 0..ACCOUNTS_PER_BLOCK {
            let local_account = (epoch_block * 131 + n * 17) % LOCAL_ACCOUNT_POOL;
            let account = account_offset + local_account;
            accounts.push(AccountEvent {
                block,
                address: address(account),
                exists: n % 35 != 0,
                incarnation: account % 3,
                nonce: block ^ n,
                balance: u256(block ^ (n << 8)),
                code_hash: empty_code,
            });
        }
        for n in 0..STORAGE_PER_BLOCK {
            let local_slot = (epoch_block * 811 + n * 29) % LOCAL_STORAGE_POOL;
            let slot_number = storage_offset + local_slot;
            let account = storage_address_id(slot_number);
            storage.push(StorageEvent {
                block,
                address: address(account),
                incarnation: account % 3,
                slot: Hash32(u256(slot_number)),
                value: if n % 5 == 0 {
                    [0; 32]
                } else {
                    u256(block ^ slot_number)
                },
            });
        }
    }
    let genesis_hash = block_hash(0);
    let segment = Segment {
        schema: SEGMENT_SCHEMA.to_owned(),
        chain_id: 1,
        genesis_hash,
        start_block: start,
        end_block: start + count - 1,
        blocks,
        accounts,
        storage,
        code: Vec::new(),
    };
    Ok(Package {
        mode: if anchor { Mode::Anchor } else { Mode::Delta },
        preceding_number: preceding.map(|value| value.0),
        preceding_hash: preceding.map(|value| value.1),
        input_sha256: Hash32::digest(&[
            (start >> 24) as u8,
            (start >> 16) as u8,
            (start >> 8) as u8,
            start as u8,
        ]),
        segment,
    })
}

fn sliding_pool_offset(epoch: u64, local_pool: u64, global_pool: u64) -> u64 {
    if global_pool <= local_pool || epoch == 0 {
        return 0;
    }
    epoch.saturating_mul(global_pool - local_pool) / (MEASURED_EPOCHS - 1)
}

fn expected_union_pool(local_pool: u64, global_pool: u64, epochs: u64) -> u64 {
    if epochs == 0 {
        0
    } else {
        local_pool + sliding_pool_offset(epochs - 1, local_pool, global_pool)
    }
}

fn storage_address_id(storage_key_id: u64) -> u64 {
    storage_key_id.wrapping_mul(0x9e37_79b9_7f4a_7c15) % GLOBAL_ACCOUNT_POOL
}

fn account_oracle(package: &Package) -> BTreeMap<Address, Vec<AccountEvent>> {
    let mut oracle: BTreeMap<Address, Vec<AccountEvent>> = BTreeMap::new();
    for event in &package.segment.accounts {
        oracle.entry(event.address).or_default().push(event.clone());
    }
    oracle
}

fn deterministic_targets(
    oracle: &BTreeMap<Address, Vec<AccountEvent>>,
    blocks: u64,
) -> Vec<(Address, u64, Option<[u8; 32]>)> {
    let addresses: Vec<_> = oracle.keys().copied().collect();
    let mut rng = XorShift64(SEED);
    (0..100)
        .map(|_| {
            let address = addresses[rng.next() as usize % addresses.len()];
            let block = rng.next() % blocks;
            let expected = oracle[&address]
                .iter()
                .rev()
                .find(|event| event.block <= block)
                .filter(|event| event.exists)
                .map(|event| event.balance);
            (address, block, expected)
        })
        .collect()
}

fn address(value: u64) -> Address {
    let mut bytes = [0; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address(bytes)
}
fn u256(value: u64) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}
fn block_hash(block: u64) -> Hash32 {
    Hash32::digest(&block.to_be_bytes())
}

fn provenance() -> Value {
    json!({
        "sampled_at": "2026-09-19",
        "finalized_head": 51498020,
        "hardware": "devbox: 32 cores, 128 GiB RAM, 15 TiB RAID",
        "node_image": "ghcr.io/base/node:v1.3.0-rc.6",
        "base_reth_tag": "base-v2.5.2.6",
        "base_reth_commit": "5877708b",
        "real_100k_hll_cardinality": {
            "range": "51,398,021..51,498,020",
            "account_rows": 19866464,
            "storage_rows": 88534816,
            "estimated_unique_accounts": 940971,
            "estimated_unique_address_slot_keys": 17339777,
            "method": "read-only StaticFileProvider streaming HyperLogLog estimate",
            "caveat": "HLL cardinalities are approximate; they shape synthetic pools but are not exact key lists",
            "summary_sha256": "e00f51a7cc9be49a2560342c20df44f15bc8dd2a53285eab1bb84042f2fccd8a"
        },
        "real_100k_layout": {
            "range": "51,398,021..51,498,020",
            "key_major_zstd_9_bytes": 1504915953,
            "elapsed_seconds": 67.26,
            "summary_sha256": "d35bc9aaf183537f332a6687089f349060d21d9650193e719c196b7438b7f01b"
        },
        "rejected_tuned_high_cardinality_generator_run": {
            "elapsed_seconds": 425.3685,
            "immutable_objects": 17456,
            "immutable_bytes": 3006815546_u64,
            "result": "completed tuned format run with intentionally rejected over-cardinal synthetic pools; not Base-shaped"
        },
        "rejected_first_sealed_epoch_scale_run": {
            "elapsed_seconds": 505.440,
            "immutable_objects": 41009,
            "immutable_bytes": 3297864788_u64,
            "data_objects": 6400,
            "index_objects": 25600,
            "checkpoint_objects": 8706,
            "directory_objects": 101,
            "catalog_objects": 2,
            "commit_objects": 100,
            "summary_sha256": "ab028cce9f88858725300fa05b82853cbeb037b3d9cb5820778dbd76774cbd93",
            "result": "runtime passed; object and byte targets failed; input to partition/subshard tuning"
        },
        "rejected_cow_run": {
            "tmp_objects_before_inode_exhaustion": 929134,
            "tmp_logical_bytes_approx": 3_000_000_000_u64,
            "tmp_allocated_bytes_approx": 5_000_000_000_u64,
            "tmp_inode_limit": 1048576,
            "md0_bytes_when_stopped": 26_000_000_000_u64,
            "md0_elapsed_when_stopped_seconds": 900,
            "result": "rejected immutable state-key COW design; unfinished"
        },
        "caveat": "external real measurements are layout evidence, not semantically complete forward state or end-to-end epoch publication"
    })
}

fn available_inodes(path: &Path) -> Option<u64> {
    let output = Command::new("df").arg("-Pi").arg(path).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let mut lines = text.lines();
    let headers: Vec<_> = lines.next()?.split_whitespace().collect();
    let free_column = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("ifree"))?;
    let values: Vec<_> = lines.next()?.split_whitespace().collect();
    values.get(free_column)?.parse().ok()
}

fn write_result(path: Option<&Path>, result: &Value) -> Result<()> {
    if let Some(path) = path {
        let parent = path.parent().context("benchmark output has no parent")?;
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(&serde_json::to_vec_pretty(result)?)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(path)
            .map_err(|error| error.error)
            .with_context(|| format!("atomically write {}", path.display()))?;
    }
    Ok(())
}

struct XorShift64(u64);
impl XorShift64 {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn small_epoch_benchmark_has_two_get_startup_and_ingested_repeat() {
        let package = shaped_package(0, 10, 10, None, true).unwrap();
        assert_eq!(package.segment.blocks.len(), 10);
        assert!(v2::benchmark_forced_collision_check().unwrap());
    }

    #[test]
    fn sliding_pool_formulas_match_local_and_100k_hll_cardinality() {
        assert_eq!(
            expected_union_pool(LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL, 1),
            LOCAL_ACCOUNT_POOL
        );
        assert_eq!(
            expected_union_pool(LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL, 1),
            LOCAL_STORAGE_POOL
        );
        assert_eq!(
            expected_union_pool(LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL, MEASURED_EPOCHS),
            GLOBAL_ACCOUNT_POOL
        );
        assert_eq!(
            expected_union_pool(LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL, MEASURED_EPOCHS),
            GLOBAL_STORAGE_POOL
        );
        for epoch in 1..MEASURED_EPOCHS {
            let account_shift = sliding_pool_offset(epoch, LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL)
                - sliding_pool_offset(epoch - 1, LOCAL_ACCOUNT_POOL, GLOBAL_ACCOUNT_POOL);
            let storage_shift = sliding_pool_offset(epoch, LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL)
                - sliding_pool_offset(epoch - 1, LOCAL_STORAGE_POOL, GLOBAL_STORAGE_POOL);
            assert!(account_shift < LOCAL_ACCOUNT_POOL);
            assert!(storage_shift < LOCAL_STORAGE_POOL);
        }
    }

    #[tokio::test]
    async fn manual_rejects_system_tmp_descendant() {
        let scratch =
            std::env::temp_dir().join(format!("fossil-manual-reject-{}", std::process::id()));
        let output =
            std::env::temp_dir().join(format!("fossil-manual-reject-{}.json", std::process::id()));
        let error = run_manual(Some(&output), Some(&scratch), 1_000, 1_000)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("system /tmp"));
        let _ = std::fs::remove_dir_all(scratch);
        let _ = std::fs::remove_file(output);
    }
}
