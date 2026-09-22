//! Fossil sealed-epoch archive format version 1.
//!
//! Publications append immutable key-major epochs. One active-window directory is
//! rewritten per sealed epoch and state is checkpointed every 64 epochs.

use crate::format::{quantity, AccountEvent, Address, BlockMeta, Hash32, StorageEvent};
use crate::normalized::{Mode, Package};
use crate::store::{ArchiveStore, VersionedBytes};
use anyhow::{anyhow, bail, Context, Result};
use moka::sync::Cache;
use sha3::{Digest as _, Keccak256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PublicationGate {
    Finalized {
        number: u64,
        hash: Hash32,
    },
    FixedOffset {
        observed_number: u64,
        observed_hash: Hash32,
        offset: u64,
    },
}

const VERSION: u8 = 1;
const HEAD_MAGIC: &[u8; 4] = b"FSEH";
const COMMIT_MAGIC: &[u8; 4] = b"FSEC";
const DATA_MAGIC: &[u8; 4] = b"FSED";
const INDEX_MAGIC: &[u8; 4] = b"FSEI";
const BLOCK_MAGIC: &[u8; 4] = b"FSEB";
const DIRECTORY_MAGIC: &[u8; 4] = b"FSER";
const CHECKPOINT_MAGIC: &[u8; 4] = b"FSEP";
const CHECKPOINT_MANIFEST_MAGIC: &[u8; 4] = b"FSEM";
const CATALOG_ROOT_MAGIC: &[u8; 4] = b"FSEW";
const CATALOG_CHUNK_MAGIC: &[u8; 4] = b"FSWC";
const CHECKPOINT_PARTITION_MAGIC: &[u8; 4] = b"FSPS";
const REF_BYTES: usize = 36;
const ROUTER_PARTITIONS: usize = 128;
const DATA_PARTITIONS: usize = 32;
const EPOCHS_PER_WINDOW: usize = 64;
const MAX_EPOCH_BLOCKS: u64 = 1_000;
// These publication caps are shared with the isolate-constrained Worker. Native
// publishers must never create a format-v1 archive that the Worker cannot read.
const MAX_DATA_DECODED: usize = 8 * 1024 * 1024;
const MAX_DATA_OBJECT_BYTES: usize = MAX_DATA_DECODED + 256 * 1024;
const MAX_CHECKPOINT_SHARD_DECODED: usize = 64 * 1024 * 1024;
const MAX_CHECKPOINT_OBJECT_BYTES: usize = MAX_CHECKPOINT_SHARD_DECODED + 1024 * 1024;
const MAX_BLOCK_DECODED: usize = 16 * 1024 * 1024;
const MAX_BLOCK_OBJECT_BYTES: usize = MAX_BLOCK_DECODED + 1024 * 1024;
const MAX_CODE_OBJECT_BYTES: usize = 1024 * 1024;
const CHECKPOINT_MANIFEST_BYTES: usize = 4 + 1 + 8 + ROUTER_PARTITIONS * REF_BYTES;
const CHECKPOINT_SHARD_TARGET: usize = 32 * 1024 * 1024;
const MIN_CHECKPOINT_SUBSHARDS: usize = 8;
const MAX_CHECKPOINT_SUBSHARDS: usize = 256;
const LEGACY_CHECKPOINT_PARTITION_MANIFEST_BYTES: usize =
    4 + 1 + MIN_CHECKPOINT_SUBSHARDS * REF_BYTES;
const MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES: usize =
    4 + 1 + 2 + MAX_CHECKPOINT_SUBSHARDS * REF_BYTES;
const CHECKPOINT_BUILD_CACHE_MAX_ITEMS: usize = 2;
const CHECKPOINT_BUILD_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 2 * 1024 * 1024;
const MAX_INDEX_ENTRIES: usize = 65_536;
const MAX_DIRECTORY_BYTES: usize = 2 * 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 2 * 1024 * 1024;
const CATALOG_CHUNK_ENTRIES: usize = 256;
const MAX_CATALOG_CHUNKS: usize = 4096;
const MAX_LOOKUP_REMOTE_GETS: u64 = 192;
const MAX_LOOKUP_DECODED_BYTES: u64 = 128 * 1024 * 1024;
const HEAD_BYTES: usize = 4 + 1 + 8 + 8 + 8 + 32 + REF_BYTES;
pub const COMMIT_BYTES: usize = 314;

const NS_ACCOUNT: u8 = 1;
const NS_STORAGE: u8 = 2;
const NS_CODE: u8 = 3;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectRef {
    pub digest: Hash32,
    pub length: u32,
}

impl ObjectRef {
    fn is_empty(self) -> bool {
        self == Self::default()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Commit {
    pub generation: u64,
    pub chain_id: u64,
    pub genesis_hash: Hash32,
    pub anchor_number: u64,
    pub anchor_hash: Hash32,
    pub published_hash: Hash32,
    pub published_number: u64,
    pub published_state_root: Hash32,
    pub parent: ObjectRef,
    pub finalized: bool,
    pub input_sha256: Hash32,
    pub active_window_start: u64,
    pub active_directory: ObjectRef,
    pub completed_catalog: ObjectRef,
}

impl Commit {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COMMIT_BYTES);
        out.extend_from_slice(COMMIT_MAGIC);
        out.push(VERSION);
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.chain_id);
        out.extend_from_slice(&self.genesis_hash.0);
        put_u64(&mut out, self.anchor_number);
        out.extend_from_slice(&self.anchor_hash.0);
        out.extend_from_slice(&self.published_hash.0);
        put_u64(&mut out, self.published_number);
        out.extend_from_slice(&self.published_state_root.0);
        put_ref(&mut out, self.parent);
        out.push(u8::from(self.finalized));
        out.extend_from_slice(&self.input_sha256.0);
        put_u64(&mut out, self.active_window_start);
        put_ref(&mut out, self.active_directory);
        put_ref(&mut out, self.completed_catalog);
        debug_assert_eq!(out.len(), COMMIT_BYTES);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != COMMIT_BYTES {
            bail!("archive commit length mismatch");
        }
        let mut input = Cursor::new(bytes);
        input.expect(COMMIT_MAGIC)?;
        if input.byte()? != VERSION {
            bail!("unsupported archive commit version");
        }
        let value = Self {
            generation: input.u64()?,
            chain_id: input.u64()?,
            genesis_hash: input.hash()?,
            anchor_number: input.u64()?,
            anchor_hash: input.hash()?,
            published_hash: input.hash()?,
            published_number: input.u64()?,
            published_state_root: input.hash()?,
            parent: input.object_ref()?,
            finalized: match input.byte()? {
                0 => false,
                1 => true,
                _ => bail!("noncanonical finalized flag"),
            },
            input_sha256: input.hash()?,
            active_window_start: input.u64()?,
            active_directory: input.object_ref()?,
            completed_catalog: input.object_ref()?,
        };
        input.end()?;
        validate_optional_ref(value.parent)?;
        validate_ref(value.active_directory)?;
        validate_optional_ref(value.completed_catalog)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Head {
    pub generation: u64,
    pub chain_id: u64,
    pub number: u64,
    pub hash: Hash32,
    pub commit: ObjectRef,
}

impl Head {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEAD_BYTES);
        out.extend_from_slice(HEAD_MAGIC);
        out.push(VERSION);
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.chain_id);
        put_u64(&mut out, self.number);
        out.extend_from_slice(&self.hash.0);
        put_ref(&mut out, self.commit);
        debug_assert_eq!(out.len(), HEAD_BYTES);
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != HEAD_BYTES {
            bail!("archive head length mismatch");
        }
        let mut input = Cursor::new(bytes);
        input.expect(HEAD_MAGIC)?;
        if input.byte()? != VERSION {
            bail!("unsupported archive head version");
        }
        let value = Self {
            generation: input.u64()?,
            chain_id: input.u64()?,
            number: input.u64()?,
            hash: input.hash()?,
            commit: input.object_ref()?,
        };
        input.end()?;
        validate_ref(value.commit)?;
        Ok(value)
    }
}

pub fn head_key(chain_id: u64) -> String {
    format!("chains/{}/heads/finalized.bin", quantity(chain_id))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Version {
    block: u64,
    value: Vec<u8>,
}

#[derive(Clone, Debug)]
struct DataObject {
    runs: Vec<(Vec<u8>, Vec<Version>)>,
}

#[derive(Clone, Debug)]
struct IndexEntry {
    fingerprint: [u8; 12],
    first_block: u64,
    dictionary_index: u8,
    run_index: u32,
}

#[derive(Clone, Debug)]
struct IndexDelta {
    partition: u8,
    epoch_start: u64,
    dictionary: Vec<ObjectRef>,
    entries: Vec<IndexEntry>,
}

#[derive(Clone, Debug)]
struct EpochDescriptor {
    start: u64,
    end: u64,
    blocks: ObjectRef,
    data: [ObjectRef; DATA_PARTITIONS],
    indexes: [ObjectRef; ROUTER_PARTITIONS],
}

#[derive(Clone, Debug)]
struct Directory {
    window_start: u64,
    base_checkpoint: ObjectRef,
    epochs: Vec<EpochDescriptor>,
}

#[derive(Clone, Copy, Debug)]
struct CheckpointManifest {
    boundary: u64,
    partitions: [ObjectRef; ROUTER_PARTITIONS],
}

#[derive(Clone, Copy, Debug)]
struct StatePointer {
    object: ObjectRef,
    run_index: u32,
    version_index: u32,
}

#[derive(Clone, Debug)]
struct CatalogEntry {
    start: u64,
    end: u64,
    directory: ObjectRef,
}

#[derive(Clone, Debug)]
struct CatalogChunk {
    entries: Vec<CatalogEntry>,
}

#[derive(Clone, Debug)]
struct CatalogRoot {
    chunks: Vec<(u64, ObjectRef)>,
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointBuildStats {
    peak_partition_entries: u64,
    peak_partition_estimated_bytes: u64,
    subshard_objects: u64,
    subshard_bytes: u64,
    partition_manifest_objects: u64,
    partition_manifest_bytes: u64,
}

#[derive(Default)]
struct CheckpointDataCache {
    objects: BTreeMap<Hash32, (DataObject, usize)>,
    order: VecDeque<Hash32>,
    decoded_bytes: usize,
}

#[derive(Default)]
struct LookupBudget {
    remote_gets: u64,
    decoded_bytes: u64,
}

impl LookupBudget {
    fn remote_get(&mut self) -> Result<()> {
        self.remote_gets += 1;
        if self.remote_gets > MAX_LOOKUP_REMOTE_GETS {
            bail!("lookup resource limit exceeded: object GET budget");
        }
        Ok(())
    }

    fn decoded(&mut self, bytes: usize) -> Result<()> {
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(bytes as u64)
            .context("lookup decoded-byte counter overflow")?;
        if self.decoded_bytes > MAX_LOOKUP_DECODED_BYTES {
            bail!("lookup resource limit exceeded: decoded-byte budget");
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Route {
    partition: u8,
    fingerprint: [u8; 12],
}

fn route_key(key: &[u8]) -> Result<Route> {
    let full = Hash32::digest(key).0;
    let partition = match key.first().copied() {
        Some(NS_ACCOUNT) | Some(NS_STORAGE) => {
            if key.len() < 21 {
                bail!("state key is too short for address routing");
            }
            Hash32::digest(&key[1..21]).0[0] >> 1
        }
        Some(NS_CODE) => Hash32::digest(&key[1..]).0[0] >> 1,
        _ => bail!("unknown state namespace"),
    };
    Ok(Route {
        partition,
        fingerprint: full[..12].try_into().unwrap(),
    })
}

fn validate_checkpoint_subshard_count(count: usize) -> Result<()> {
    if !(MIN_CHECKPOINT_SUBSHARDS..=MAX_CHECKPOINT_SUBSHARDS).contains(&count)
        || !count.is_power_of_two()
    {
        bail!(
            "checkpoint subshard count must be a power of two between {} and {}",
            MIN_CHECKPOINT_SUBSHARDS,
            MAX_CHECKPOINT_SUBSHARDS
        );
    }
    Ok(())
}

fn checkpoint_subshard(key: &[u8], count: usize) -> Result<usize> {
    validate_checkpoint_subshard_count(count)?;
    let hash = Hash32::digest(key).0;
    Ok(hash[12] as usize >> (8 - count.trailing_zeros() as usize))
}

fn full_key(namespace: u8, logical: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + logical.len());
    key.push(namespace);
    key.extend_from_slice(logical);
    key
}

fn encode_data(runs: &BTreeMap<Vec<u8>, Vec<Version>>) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_uleb(&mut body, runs.len() as u64);
    for (key, versions) in runs {
        put_bytes(&mut body, key);
        put_uleb(&mut body, versions.len() as u64);
        let mut previous = 0_u64;
        for (index, version) in versions.iter().enumerate() {
            if index > 0 && version.block <= previous {
                bail!("epoch versions are not strictly sorted");
            }
            put_uleb(&mut body, version.block - previous);
            put_bytes(&mut body, &version.value);
            previous = version.block;
        }
    }
    let encoded = encode_compressed(DATA_MAGIC, &body, MAX_DATA_DECODED)
        .context("data object exceeds Worker 8 MiB decoded publication cap")?;
    if encoded.len() > MAX_DATA_OBJECT_BYTES {
        bail!("data object exceeds Worker 8.25 MiB encoded publication cap");
    }
    Ok(encoded)
}

fn decode_data(reference: ObjectRef, bytes: &[u8]) -> Result<DataObject> {
    let decoded = decode_compressed(reference, bytes, DATA_MAGIC, MAX_DATA_DECODED)?;
    let mut input = Cursor::new(&decoded);
    let count = bounded_count(&mut input, 3, "data runs")?;
    let mut runs = Vec::new();
    runs.try_reserve_exact(count).context("reserve data runs")?;
    let mut prior_key: Option<Vec<u8>> = None;
    for _ in 0..count {
        let key = input.bytes()?.to_vec();
        if key.is_empty() || prior_key.as_ref().is_some_and(|prior| prior >= &key) {
            bail!("data run keys are not strictly sorted");
        }
        let version_count = bounded_count(&mut input, 2, "data versions")?;
        if version_count == 0 {
            bail!("data run has zero versions");
        }
        let mut versions = Vec::new();
        versions
            .try_reserve_exact(version_count)
            .context("reserve data versions")?;
        let mut block = 0_u64;
        for _ in 0..version_count {
            let delta = input.uleb()?;
            block = block.checked_add(delta).context("version block overflow")?;
            if versions
                .last()
                .is_some_and(|prior: &Version| prior.block >= block)
            {
                bail!("data versions are not strictly sorted");
            }
            versions.push(Version {
                block,
                value: input.bytes()?.to_vec(),
            });
        }
        prior_key = Some(key.clone());
        runs.push((key, versions));
    }
    input.end()?;
    Ok(DataObject { runs })
}

fn encode_blocks(blocks: &[BlockMeta]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_uleb(&mut body, blocks.len() as u64);
    for block in blocks {
        put_u64(&mut body, block.number);
        body.extend_from_slice(&block.hash.0);
        body.extend_from_slice(&block.parent_hash.0);
        body.extend_from_slice(&block.state_root.0);
        put_uleb(&mut body, block.timestamp);
    }
    encode_compressed(BLOCK_MAGIC, &body, MAX_BLOCK_DECODED)
}

pub fn decode_blocks(reference: ObjectRef, bytes: &[u8]) -> Result<Vec<BlockMeta>> {
    if reference.length as usize > MAX_BLOCK_OBJECT_BYTES {
        bail!("block metadata reference exceeds pre-download bound");
    }
    let decoded = decode_compressed(reference, bytes, BLOCK_MAGIC, MAX_BLOCK_DECODED)?;
    let mut input = Cursor::new(&decoded);
    let count = bounded_count(&mut input, 8 + 32 * 3 + 1, "block metadata")?;
    let mut blocks = Vec::new();
    blocks.try_reserve_exact(count)?;
    for _ in 0..count {
        blocks.push(BlockMeta {
            number: input.u64()?,
            hash: input.hash()?,
            parent_hash: input.hash()?,
            state_root: input.hash()?,
            timestamp: input.uleb()?,
        });
    }
    input.end()?;
    for pair in blocks.windows(2) {
        if pair[0].number.checked_add(1) != Some(pair[1].number) {
            bail!("block metadata is not contiguous");
        }
    }
    Ok(blocks)
}

fn encode_index(index: &IndexDelta) -> Result<Vec<u8>> {
    if index.entries.len() > MAX_INDEX_ENTRIES {
        bail!("index delta exceeds Worker 65,536-entry publication cap");
    }
    let mut out = Vec::new();
    out.extend_from_slice(INDEX_MAGIC);
    out.push(VERSION);
    out.push(index.partition);
    put_u64(&mut out, index.epoch_start);
    put_uleb(&mut out, index.dictionary.len() as u64);
    for reference in &index.dictionary {
        validate_ref(*reference)?;
        put_ref(&mut out, *reference);
    }
    put_uleb(&mut out, index.entries.len() as u64);
    for entry in &index.entries {
        out.extend_from_slice(&entry.fingerprint);
        put_uleb(
            &mut out,
            entry
                .first_block
                .checked_sub(index.epoch_start)
                .context("index first change predates epoch")?,
        );
        out.push(entry.dictionary_index);
        put_u32(&mut out, entry.run_index);
    }
    if out.len() > MAX_INDEX_BYTES {
        bail!("index delta exceeds Worker 2 MiB encoded publication cap");
    }
    Ok(out)
}

fn decode_index(reference: ObjectRef, bytes: &[u8]) -> Result<IndexDelta> {
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_INDEX_BYTES {
        bail!("index delta exceeds bound");
    }
    let mut input = Cursor::new(bytes);
    input.expect(INDEX_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported index delta version");
    }
    let partition = input.byte()?;
    let epoch_start = input.u64()?;
    let dictionary_count = bounded_count(&mut input, REF_BYTES, "index dictionary")?;
    if dictionary_count > 4 {
        bail!("index dictionary exceeds data fanout");
    }
    let mut dictionary = Vec::new();
    dictionary.try_reserve_exact(dictionary_count)?;
    for _ in 0..dictionary_count {
        let item = input.object_ref()?;
        validate_ref(item)?;
        dictionary.push(item);
    }
    let entry_count = bounded_count(&mut input, 18, "index entries")?;
    if entry_count > MAX_INDEX_ENTRIES {
        bail!("index delta exceeds shared 65,536-entry format cap");
    }
    let mut entries = Vec::new();
    entries.try_reserve_exact(entry_count)?;
    for _ in 0..entry_count {
        let fingerprint = input.take(12)?.try_into().unwrap();
        let first_block = epoch_start
            .checked_add(input.uleb()?)
            .context("index first change overflow")?;
        let dictionary_index = input.byte()?;
        let run_index = input.u32()?;
        if dictionary_index as usize >= dictionary.len() {
            bail!("index dictionary reference out of range");
        }
        entries.push(IndexEntry {
            fingerprint,
            first_block,
            dictionary_index,
            run_index,
        });
    }
    input.end()?;
    if !entries.windows(2).all(|pair| {
        (pair[0].fingerprint, pair[0].first_block, pair[0].run_index)
            <= (pair[1].fingerprint, pair[1].first_block, pair[1].run_index)
    }) {
        bail!("index entries are not sorted");
    }
    Ok(IndexDelta {
        partition,
        epoch_start,
        dictionary,
        entries,
    })
}

fn encode_directory(directory: &Directory) -> Result<Vec<u8>> {
    if directory.epochs.len() > EPOCHS_PER_WINDOW {
        bail!("active window has too many epochs");
    }
    let mut out = Vec::new();
    out.extend_from_slice(DIRECTORY_MAGIC);
    out.push(VERSION);
    put_u64(&mut out, directory.window_start);
    put_ref(&mut out, directory.base_checkpoint);
    out.push(directory.epochs.len() as u8);
    for epoch in &directory.epochs {
        put_u64(&mut out, epoch.start);
        put_u64(&mut out, epoch.end);
        put_ref(&mut out, epoch.blocks);
        for reference in epoch.data {
            put_ref(&mut out, reference);
        }
        for reference in epoch.indexes {
            put_ref(&mut out, reference);
        }
    }
    if out.len() > MAX_DIRECTORY_BYTES {
        bail!("window directory exceeds bound");
    }
    Ok(out)
}

fn decode_directory(reference: ObjectRef, bytes: &[u8]) -> Result<Directory> {
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_DIRECTORY_BYTES {
        bail!("window directory exceeds bound");
    }
    let mut input = Cursor::new(bytes);
    input.expect(DIRECTORY_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported directory version");
    }
    let window_start = input.u64()?;
    let base_checkpoint = input.object_ref()?;
    validate_optional_ref(base_checkpoint)?;
    let count = input.byte()? as usize;
    if count > EPOCHS_PER_WINDOW {
        bail!("directory epoch count exceeds window bound");
    }
    let mut epochs = Vec::new();
    epochs.try_reserve_exact(count)?;
    let mut prior_end = None;
    for _ in 0..count {
        let start = input.u64()?;
        let end = input.u64()?;
        let expected_start = prior_end
            .map(|prior: u64| prior.checked_add(1).context("directory block overflow"))
            .transpose()?;
        if end < start || expected_start.map_or(start != window_start, |expected| start != expected)
        {
            bail!("directory epochs are not contiguous");
        }
        let blocks = input.object_ref()?;
        validate_ref(blocks)?;
        let mut data = [ObjectRef::default(); DATA_PARTITIONS];
        for item in &mut data {
            *item = input.object_ref()?;
            validate_optional_ref(*item)?;
        }
        let mut indexes = [ObjectRef::default(); ROUTER_PARTITIONS];
        for item in &mut indexes {
            *item = input.object_ref()?;
            validate_optional_ref(*item)?;
        }
        epochs.push(EpochDescriptor {
            start,
            end,
            blocks,
            data,
            indexes,
        });
        prior_end = Some(end);
    }
    input.end()?;
    Ok(Directory {
        window_start,
        base_checkpoint,
        epochs,
    })
}

fn uleb_encoded_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn checkpoint_entry_decoded_len(key: &[u8]) -> Result<usize> {
    uleb_encoded_len(key.len())
        .checked_add(key.len())
        .and_then(|length| length.checked_add(REF_BYTES + 8))
        .context("checkpoint entry decoded length overflow")
}

fn select_checkpoint_subshard_count(
    entries: &BTreeMap<Vec<u8>, StatePointer>,
    target: usize,
) -> Result<usize> {
    let mut count = MIN_CHECKPOINT_SUBSHARDS;
    loop {
        let mut item_counts = vec![0usize; count];
        let mut lengths = vec![0usize; count];
        for key in entries.keys() {
            let shard = checkpoint_subshard(key, count)?;
            item_counts[shard] += 1;
            lengths[shard] = lengths[shard]
                .checked_add(checkpoint_entry_decoded_len(key)?)
                .context("checkpoint shard decoded length overflow")?;
        }
        let largest = lengths
            .into_iter()
            .zip(item_counts)
            .map(|(length, items)| length + uleb_encoded_len(items))
            .max()
            .unwrap_or(1);
        if largest <= target {
            return Ok(count);
        }
        if count == MAX_CHECKPOINT_SUBSHARDS {
            bail!(
                "checkpoint partition capacity exceeded: a shard remains {} bytes with {} subshards ({} MiB decoded writer target)",
                largest,
                MAX_CHECKPOINT_SUBSHARDS,
                CHECKPOINT_SHARD_TARGET / (1024 * 1024)
            );
        }
        count *= 2;
    }
}

fn encode_checkpoint_partition(entries: &BTreeMap<Vec<u8>, StatePointer>) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    put_uleb(&mut body, entries.len() as u64);
    for (key, pointer) in entries {
        put_bytes(&mut body, key);
        put_ref(&mut body, pointer.object);
        put_u32(&mut body, pointer.run_index);
        put_u32(&mut body, pointer.version_index);
    }
    if body.len() > CHECKPOINT_SHARD_TARGET {
        bail!("checkpoint subshard exceeds 32 MiB target");
    }
    encode_compressed(CHECKPOINT_MAGIC, &body, MAX_CHECKPOINT_SHARD_DECODED)
}

fn decode_checkpoint_partition(
    reference: ObjectRef,
    bytes: &[u8],
) -> Result<BTreeMap<Vec<u8>, StatePointer>> {
    let decoded = decode_compressed(
        reference,
        bytes,
        CHECKPOINT_MAGIC,
        MAX_CHECKPOINT_SHARD_DECODED,
    )?;
    let mut input = Cursor::new(&decoded);
    let count = bounded_count(&mut input, 1 + REF_BYTES + 8, "checkpoint entries")?;
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        let key = input.bytes()?.to_vec();
        let pointer = StatePointer {
            object: input.object_ref()?,
            run_index: input.u32()?,
            version_index: input.u32()?,
        };
        validate_ref(pointer.object)?;
        if entries.insert(key, pointer).is_some() {
            bail!("duplicate checkpoint key");
        }
    }
    input.end()?;
    Ok(entries)
}

fn encode_checkpoint_partition_manifest(subshards: &[ObjectRef]) -> Result<Vec<u8>> {
    validate_checkpoint_subshard_count(subshards.len())?;
    let mut out = Vec::new();
    out.extend_from_slice(CHECKPOINT_PARTITION_MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&(subshards.len() as u16).to_be_bytes());
    for reference in subshards {
        put_ref(&mut out, *reference);
    }
    Ok(out)
}

fn decode_checkpoint_partition_manifest(
    reference: ObjectRef,
    bytes: &[u8],
) -> Result<Vec<ObjectRef>> {
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES {
        bail!("checkpoint partition manifest exceeds bound");
    }
    let mut input = Cursor::new(bytes);
    input.expect(CHECKPOINT_PARTITION_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported checkpoint partition manifest version");
    }
    let (count, expected_bytes) = if bytes.len() == LEGACY_CHECKPOINT_PARTITION_MANIFEST_BYTES {
        (
            MIN_CHECKPOINT_SUBSHARDS,
            LEGACY_CHECKPOINT_PARTITION_MANIFEST_BYTES,
        )
    } else {
        let count = input.u16()? as usize;
        validate_checkpoint_subshard_count(count)?;
        (count, 4 + 1 + 2 + count * REF_BYTES)
    };
    if bytes.len() != expected_bytes {
        bail!("checkpoint partition manifest length does not match subshard count");
    }
    let mut subshards = Vec::new();
    subshards.try_reserve_exact(count)?;
    for _ in 0..count {
        let item = input.object_ref()?;
        validate_optional_ref(item)?;
        subshards.push(item);
    }
    input.end()?;
    Ok(subshards)
}

fn encode_checkpoint_manifest(
    boundary: u64,
    partitions: &[ObjectRef; ROUTER_PARTITIONS],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(CHECKPOINT_MANIFEST_MAGIC);
    out.push(VERSION);
    put_u64(&mut out, boundary);
    for reference in partitions {
        put_ref(&mut out, *reference);
    }
    out
}

fn decode_checkpoint_manifest(reference: ObjectRef, bytes: &[u8]) -> Result<CheckpointManifest> {
    verify_object(reference, bytes)?;
    let mut input = Cursor::new(bytes);
    input.expect(CHECKPOINT_MANIFEST_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported checkpoint manifest version");
    }
    let boundary = input.u64()?;
    let mut partitions = [ObjectRef::default(); ROUTER_PARTITIONS];
    for item in &mut partitions {
        *item = input.object_ref()?;
        validate_optional_ref(*item)?;
    }
    input.end()?;
    Ok(CheckpointManifest {
        boundary,
        partitions,
    })
}

fn encode_catalog_chunk(chunk: &CatalogChunk) -> Result<Vec<u8>> {
    if chunk.entries.is_empty() || chunk.entries.len() > CATALOG_CHUNK_ENTRIES {
        bail!("catalog chunk entry count is out of bounds");
    }
    let mut out = Vec::new();
    out.extend_from_slice(CATALOG_CHUNK_MAGIC);
    out.push(VERSION);
    put_uleb(&mut out, chunk.entries.len() as u64);
    for entry in &chunk.entries {
        put_u64(&mut out, entry.start);
        put_u64(&mut out, entry.end);
        put_ref(&mut out, entry.directory);
    }
    if out.len() > MAX_CATALOG_BYTES {
        bail!("catalog chunk exceeds bound");
    }
    Ok(out)
}

fn decode_catalog_chunk(reference: ObjectRef, bytes: &[u8]) -> Result<CatalogChunk> {
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_CATALOG_BYTES {
        bail!("catalog chunk exceeds bound");
    }
    let mut input = Cursor::new(bytes);
    input.expect(CATALOG_CHUNK_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported catalog chunk version");
    }
    let count = bounded_count(&mut input, 16 + REF_BYTES, "catalog entries")?;
    if count == 0 || count > CATALOG_CHUNK_ENTRIES {
        bail!("catalog chunk entry count is out of bounds");
    }
    let mut entries = Vec::new();
    entries.try_reserve_exact(count)?;
    for _ in 0..count {
        let entry = CatalogEntry {
            start: input.u64()?,
            end: input.u64()?,
            directory: input.object_ref()?,
        };
        validate_ref(entry.directory)?;
        if entry.end < entry.start {
            bail!("invalid catalog range");
        }
        entries.push(entry);
    }
    input.end()?;
    if !entries.windows(2).all(|pair| pair[0].end < pair[1].start) {
        bail!("catalog ranges overlap");
    }
    Ok(CatalogChunk { entries })
}

fn encode_catalog_root(root: &CatalogRoot) -> Result<Vec<u8>> {
    if root.chunks.is_empty() || root.chunks.len() > MAX_CATALOG_CHUNKS {
        bail!("catalog root chunk count is out of bounds");
    }
    let mut out = Vec::new();
    out.extend_from_slice(CATALOG_ROOT_MAGIC);
    out.push(VERSION);
    put_uleb(&mut out, root.chunks.len() as u64);
    for (start, reference) in &root.chunks {
        put_u64(&mut out, *start);
        put_ref(&mut out, *reference);
    }
    if out.len() > MAX_CATALOG_BYTES {
        bail!("catalog root exceeds bound");
    }
    Ok(out)
}

fn decode_catalog_root(reference: ObjectRef, bytes: &[u8]) -> Result<CatalogRoot> {
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_CATALOG_BYTES {
        bail!("catalog root exceeds bound");
    }
    let mut input = Cursor::new(bytes);
    input.expect(CATALOG_ROOT_MAGIC)?;
    if input.byte()? != VERSION {
        bail!("unsupported catalog root version");
    }
    let count = bounded_count(&mut input, 8 + REF_BYTES, "catalog root chunks")?;
    if count == 0 || count > MAX_CATALOG_CHUNKS {
        bail!("catalog root chunk count is out of bounds");
    }
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(count)?;
    for _ in 0..count {
        let start = input.u64()?;
        let item = input.object_ref()?;
        validate_ref(item)?;
        chunks.push((start, item));
    }
    input.end()?;
    if !chunks.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        bail!("catalog root starts are not sorted");
    }
    Ok(CatalogRoot { chunks })
}

fn encode_compressed(magic: &[u8; 4], decoded: &[u8], limit: usize) -> Result<Vec<u8>> {
    if decoded.len() > limit {
        bail!("decoded epoch object exceeds limit");
    }
    let encoded = zstd::encode_all(decoded, 9)?;
    let mut out = Vec::with_capacity(13 + encoded.len());
    out.extend_from_slice(magic);
    out.push(VERSION);
    put_u32(&mut out, decoded.len() as u32);
    put_u32(&mut out, encoded.len() as u32);
    out.extend_from_slice(&encoded);
    if out.len() > limit.saturating_add(1024 * 1024) {
        bail!("compressed epoch object exceeds encoded bound");
    }
    Ok(out)
}

fn compressed_decoded_len(bytes: &[u8], limit: usize) -> Result<usize> {
    if bytes.len() < 13 {
        bail!("truncated compressed object");
    }
    let decoded = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    if decoded > limit {
        bail!("compressed object decoded length exceeds bound");
    }
    Ok(decoded)
}

fn decode_compressed(
    reference: ObjectRef,
    bytes: &[u8],
    magic: &[u8; 4],
    limit: usize,
) -> Result<Vec<u8>> {
    verify_object(reference, bytes)?;
    let mut input = Cursor::new(bytes);
    input.expect(magic)?;
    if input.byte()? != VERSION {
        bail!("unsupported compressed object version");
    }
    let decoded_len = input.u32()? as usize;
    let encoded_len = input.u32()? as usize;
    if decoded_len > limit || encoded_len != input.remaining() {
        bail!("compressed object length mismatch");
    }
    let payload = input.take(encoded_len)?;
    input.end()?;
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(decoded_len)?;
    zstd::Decoder::new(payload)?
        .take((limit + 1) as u64)
        .read_to_end(&mut decoded)?;
    if decoded.len() != decoded_len {
        bail!("compressed object decoded length mismatch");
    }
    Ok(decoded)
}

async fn put_object(store: &dyn ArchiveStore, bytes: &[u8]) -> Result<ObjectRef> {
    let reference = ObjectRef {
        digest: Hash32::digest(bytes),
        length: bytes
            .len()
            .try_into()
            .context("immutable object exceeds u32")?,
    };
    store
        .put_immutable(&reference.digest.object_key(), bytes)
        .await?;
    Ok(reference)
}

async fn get_verified(
    store: &dyn ArchiveStore,
    reference: ObjectRef,
    maximum: usize,
) -> Result<Vec<u8>> {
    validate_ref(reference)?;
    if reference.length as usize > maximum {
        bail!("archive object reference length exceeds pre-download bound");
    }
    let bytes = store
        .get_bounded(&reference.digest.object_key(), reference.length as usize)
        .await?;
    verify_object(reference, &bytes)?;
    Ok(bytes)
}

#[derive(Clone, Debug)]
pub struct PublishOutcome {
    pub commit: Hash32,
    pub generation: u64,
    pub published_number: u64,
    pub published_hash: Hash32,
    pub idempotent: bool,
    pub checkpoint_peak_partition_entries: u64,
    pub checkpoint_peak_partition_estimated_bytes: u64,
    pub checkpoint_subshard_objects: u64,
    pub checkpoint_subshard_bytes: u64,
    pub checkpoint_partition_manifest_objects: u64,
    pub checkpoint_partition_manifest_bytes: u64,
}

pub async fn publish(
    store: Arc<dyn ArchiveStore>,
    package: Package,
    gate: PublicationGate,
) -> Result<PublishOutcome> {
    let chain_id = package.segment.chain_id;
    let key = head_key(chain_id);
    let current = store.read_mutable_bounded(&key, HEAD_BYTES).await?;
    let previous = match &current {
        Some(value) => Some(load_from_head(store.as_ref(), value, chain_id).await?),
        None => None,
    };
    let (eligible, finalized) = gate_fields(&gate);
    let eligible = eligible.min(package.segment.end_block);
    validate_checkpoint(&package, previous.as_ref().map(|x| &x.1), &gate)?;
    if eligible < package.segment.start_block {
        bail!("publication gate does not make any package block eligible");
    }
    if let Some((head, commit)) = &previous {
        if commit.input_sha256 == package.input_sha256 && commit.published_number >= eligible {
            return Ok(PublishOutcome {
                commit: head.commit.digest,
                generation: commit.generation,
                published_number: commit.published_number,
                published_hash: commit.published_hash,
                idempotent: true,
                checkpoint_peak_partition_entries: 0,
                checkpoint_peak_partition_estimated_bytes: 0,
                checkpoint_subshard_objects: 0,
                checkpoint_subshard_bytes: 0,
                checkpoint_partition_manifest_objects: 0,
                checkpoint_partition_manifest_bytes: 0,
            });
        }
    }
    validate_transition(&package, previous.as_ref().map(|x| &x.1))?;
    if previous
        .as_ref()
        .is_some_and(|(_, commit)| gate_fields(&gate).0 < commit.published_number)
    {
        bail!("publication gate is behind existing immutable head");
    }
    let checkpoint_blocks = package.segment.blocks.clone();
    let segment = truncate(package.segment, eligible)?;
    if segment.end_block - segment.start_block + 1 > MAX_EPOCH_BLOCKS {
        bail!("archive exceeds 1,000 blocks; producer must seal bounded epochs");
    }

    let (mut directory, mut catalog) = match &previous {
        None => (
            Directory {
                window_start: segment.start_block,
                base_checkpoint: ObjectRef::default(),
                epochs: Vec::new(),
            },
            ObjectRef::default(),
        ),
        Some((_, commit)) => {
            let bytes =
                get_verified(store.as_ref(), commit.active_directory, MAX_DIRECTORY_BYTES).await?;
            let directory = decode_directory(commit.active_directory, &bytes)?;
            validate_active_directory(commit, &directory)?;
            (directory, commit.completed_catalog)
        }
    };

    let mut changes: BTreeMap<Vec<u8>, Vec<Version>> = BTreeMap::new();
    for event in &segment.accounts {
        push_change(
            &mut changes,
            full_key(NS_ACCOUNT, &event.address.0),
            event.block,
            encode_account(event),
        )?;
    }
    for event in &segment.storage {
        let mut logical = event.address.0.to_vec();
        logical.extend_from_slice(&event.incarnation.to_be_bytes());
        logical.extend_from_slice(&event.slot.0);
        push_change(
            &mut changes,
            full_key(NS_STORAGE, &logical),
            event.block,
            encode_storage(event),
        )?;
    }
    let blocks = put_object(store.as_ref(), &encode_blocks(&segment.blocks)?).await?;

    let prior_publication = previous.as_ref().map(|(head, commit)| {
        Publication::from_loaded(store.clone(), head.clone(), commit.clone())
    });
    for blob in &segment.code {
        if blob.bytes.len() > MAX_CODE_OBJECT_BYTES {
            bail!("code blob exceeds 1 MiB prototype limit");
        }
        let object = ObjectRef {
            digest: Hash32::digest(&blob.bytes),
            length: blob.bytes.len().try_into()?,
        };
        let logical = blob.code_hash.0.to_vec();
        let key = full_key(NS_CODE, &logical);
        let existing = match &prior_publication {
            Some(publication) => {
                publication
                    .lookup_raw(&key, publication.commit.published_number)
                    .await?
            }
            None => None,
        };
        let value = if let Some((_, value)) = existing {
            let (first_seen, old_object) = decode_code_meta(&value)?;
            if old_object != object {
                bail!(
                    "conflicting bytes for previously published code {}",
                    blob.code_hash
                );
            }
            if blob.first_seen_block >= first_seen {
                continue;
            }
            encode_code_meta(blob.first_seen_block, old_object)
        } else {
            store
                .put_immutable(&object.digest.object_key(), &blob.bytes)
                .await?;
            encode_code_meta(blob.first_seen_block, object)
        };
        push_change(&mut changes, key, blob.first_seen_block, value)?;
    }

    validate_code_references(prior_publication.as_ref(), &changes, &segment.accounts).await?;
    let descriptor = seal_epoch(
        store.as_ref(),
        segment.start_block,
        segment.end_block,
        blocks,
        changes,
    )
    .await?;
    directory.epochs.push(descriptor);
    let mut checkpoint_stats = CheckpointBuildStats::default();
    let completed_directory;
    if directory.epochs.len() == EPOCHS_PER_WINDOW {
        let completed_bytes = encode_directory(&directory)?;
        completed_directory = put_object(store.as_ref(), &completed_bytes).await?;
        let (checkpoint, closing_stats) =
            build_checkpoint(store.as_ref(), directory.base_checkpoint, &directory.epochs).await?;
        checkpoint_stats = closing_stats;
        catalog = append_catalog(
            store.as_ref(),
            catalog,
            CatalogEntry {
                start: directory.window_start,
                end: segment.end_block,
                directory: completed_directory,
            },
        )
        .await?;
        directory = Directory {
            window_start: segment.end_block + 1,
            base_checkpoint: checkpoint,
            epochs: Vec::new(),
        };
    }
    let directory_bytes = encode_directory(&directory)?;
    let active_directory = put_object(store.as_ref(), &directory_bytes).await?;

    let published = segment.blocks.last().context("empty publication")?;
    let old = previous.as_ref().map(|x| &x.1);
    let commit = Commit {
        generation: old.map_or(1, |commit| commit.generation + 1),
        chain_id,
        genesis_hash: segment.genesis_hash,
        anchor_number: old.map_or(segment.start_block, |commit| commit.anchor_number),
        anchor_hash: old.map_or(segment.blocks[0].hash, |commit| commit.anchor_hash),
        published_hash: published.hash,
        published_number: published.number,
        published_state_root: published.state_root,
        parent: previous
            .as_ref()
            .map_or(ObjectRef::default(), |(head, _)| head.commit),
        finalized,
        input_sha256: package.input_sha256,
        active_window_start: directory.window_start,
        active_directory,
        completed_catalog: catalog,
    };
    let commit_bytes = commit.encode();
    let commit_ref = put_object(store.as_ref(), &commit_bytes).await?;
    validate_checkpoint_segment(&checkpoint_blocks, old, &gate)?;
    let head = Head {
        generation: commit.generation,
        chain_id,
        number: commit.published_number,
        hash: commit.published_hash,
        commit: commit_ref,
    };
    store
        .compare_and_swap(&key, current.as_ref(), &head.encode())
        .await?;
    Ok(PublishOutcome {
        commit: commit_ref.digest,
        generation: commit.generation,
        published_number: commit.published_number,
        published_hash: commit.published_hash,
        idempotent: false,
        checkpoint_peak_partition_entries: checkpoint_stats.peak_partition_entries,
        checkpoint_peak_partition_estimated_bytes: checkpoint_stats.peak_partition_estimated_bytes,
        checkpoint_subshard_objects: checkpoint_stats.subshard_objects,
        checkpoint_subshard_bytes: checkpoint_stats.subshard_bytes,
        checkpoint_partition_manifest_objects: checkpoint_stats.partition_manifest_objects,
        checkpoint_partition_manifest_bytes: checkpoint_stats.partition_manifest_bytes,
    })
}

fn push_change(
    changes: &mut BTreeMap<Vec<u8>, Vec<Version>>,
    key: Vec<u8>,
    block: u64,
    value: Vec<u8>,
) -> Result<()> {
    let versions = changes.entry(key).or_default();
    if versions.last().is_some_and(|prior| prior.block >= block) {
        bail!("epoch key versions are not strictly sorted");
    }
    versions.push(Version { block, value });
    Ok(())
}

async fn seal_epoch(
    store: &dyn ArchiveStore,
    start: u64,
    end: u64,
    blocks: ObjectRef,
    changes: BTreeMap<Vec<u8>, Vec<Version>>,
) -> Result<EpochDescriptor> {
    let mut groups: Vec<BTreeMap<Vec<u8>, Vec<Version>>> =
        (0..DATA_PARTITIONS).map(|_| BTreeMap::new()).collect();
    for (key, versions) in changes {
        let route = route_key(&key)?;
        groups[(route.partition >> 2) as usize].insert(key, versions);
    }
    let mut data = [ObjectRef::default(); DATA_PARTITIONS];
    let mut mappings: BTreeMap<Vec<u8>, (ObjectRef, u32, u64, [u8; 12])> = BTreeMap::new();
    for (partition, runs) in groups.into_iter().enumerate() {
        if runs.is_empty() {
            continue;
        }
        let bytes = encode_data(&runs)?;
        let reference = put_object(store, &bytes).await?;
        data[partition] = reference;
        for (run_index, (key, versions)) in runs.iter().enumerate() {
            let route = route_key(key)?;
            mappings.insert(
                key.clone(),
                (
                    reference,
                    run_index.try_into()?,
                    versions[0].block,
                    route.fingerprint,
                ),
            );
        }
    }
    let mut index_groups: Vec<Vec<IndexEntry>> =
        (0..ROUTER_PARTITIONS).map(|_| Vec::new()).collect();
    let mut dictionaries = [ObjectRef::default(); ROUTER_PARTITIONS];
    for (key, (reference, run_index, first_block, fingerprint)) in mappings {
        let partition = route_key(&key)?.partition as usize;
        dictionaries[partition] = reference;
        index_groups[partition].push(IndexEntry {
            fingerprint,
            first_block,
            dictionary_index: 0,
            run_index,
        });
    }
    let mut indexes = [ObjectRef::default(); ROUTER_PARTITIONS];
    for (partition, mut entries) in index_groups.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        entries.sort_by_key(|entry| (entry.fingerprint, entry.first_block, entry.run_index));
        let bytes = encode_index(&IndexDelta {
            partition: partition as u8,
            epoch_start: start,
            dictionary: vec![dictionaries[partition]],
            entries,
        })?;
        indexes[partition] = put_object(store, &bytes).await?;
    }
    Ok(EpochDescriptor {
        start,
        end,
        blocks,
        data,
        indexes,
    })
}

async fn build_checkpoint(
    store: &dyn ArchiveStore,
    base_checkpoint: ObjectRef,
    epochs: &[EpochDescriptor],
) -> Result<(ObjectRef, CheckpointBuildStats)> {
    let base_manifest = if base_checkpoint.is_empty() {
        None
    } else {
        let bytes = get_verified(store, base_checkpoint, CHECKPOINT_MANIFEST_BYTES).await?;
        Some(decode_checkpoint_manifest(base_checkpoint, &bytes)?)
    };
    if let (Some(base), Some(first)) = (base_manifest.as_ref(), epochs.first()) {
        if base.boundary.checked_add(1) != Some(first.start) {
            bail!("checkpoint boundary is not immediately before applied epochs");
        }
    }
    let base_partitions = base_manifest
        .map(|manifest| manifest.partitions)
        .unwrap_or([ObjectRef::default(); ROUTER_PARTITIONS]);
    let mut output_partitions = [ObjectRef::default(); ROUTER_PARTITIONS];
    let mut stats = CheckpointBuildStats::default();
    for partition in 0..ROUTER_PARTITIONS {
        let mut state: BTreeMap<Vec<u8>, StatePointer> = BTreeMap::new();
        if !base_partitions[partition].is_empty() {
            let manifest_ref = base_partitions[partition];
            let bytes =
                get_verified(store, manifest_ref, MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES).await?;
            let subshards = decode_checkpoint_partition_manifest(manifest_ref, &bytes)?;
            for reference in subshards.into_iter().filter(|item| !item.is_empty()) {
                let bytes = get_verified(store, reference, MAX_CHECKPOINT_OBJECT_BYTES).await?;
                state.extend(decode_checkpoint_partition(reference, &bytes)?);
            }
        }
        for epoch in epochs {
            let reference = epoch.data[partition >> 2];
            if reference.is_empty() {
                continue;
            }
            let bytes = get_verified(store, reference, MAX_DATA_OBJECT_BYTES).await?;
            let data = decode_data(reference, &bytes)?;
            for (run_index, (key, versions)) in data.runs.iter().enumerate() {
                if route_key(key)?.partition as usize != partition {
                    continue;
                }
                let version_index = versions.len() - 1;
                if is_default(key, &versions[version_index].value)? {
                    state.remove(key);
                } else {
                    state.insert(
                        key.clone(),
                        StatePointer {
                            object: reference,
                            run_index: run_index.try_into()?,
                            version_index: version_index.try_into()?,
                        },
                    );
                }
            }
        }

        // Accounts and every storage incarnation share this primary partition.
        // Resolve final account state, then retain storage only for a live account's
        // final incarnation. Historical epoch objects remain untouched.
        let mut decoded_cache = CheckpointDataCache::default();
        let mut accounts: BTreeMap<[u8; 20], u64> = BTreeMap::new();
        for (key, pointer) in state
            .iter()
            .filter(|(key, _)| key.first() == Some(&NS_ACCOUNT))
        {
            let value = pointer_value(store, *pointer, &mut decoded_cache).await?;
            let address: [u8; 20] = key[1..21].try_into().unwrap();
            let account = decode_account(0, Address(address), &value)?;
            if account.exists {
                accounts.insert(address, account.incarnation);
            }
        }
        state.retain(|key, _| {
            if key.first() != Some(&NS_STORAGE) || key.len() < 29 {
                return true;
            }
            let address: [u8; 20] = key[1..21].try_into().unwrap();
            let incarnation = u64::from_be_bytes(key[21..29].try_into().unwrap());
            accounts.get(&address) == Some(&incarnation)
        });

        let estimated_bytes: u64 = state
            .keys()
            .map(|key| (key.len() + REF_BYTES + 8) as u64)
            .sum();
        stats.peak_partition_entries = stats.peak_partition_entries.max(state.len() as u64);
        stats.peak_partition_estimated_bytes =
            stats.peak_partition_estimated_bytes.max(estimated_bytes);

        let subshard_count = select_checkpoint_subshard_count(&state, CHECKPOINT_SHARD_TARGET)?;
        let mut subgroups: Vec<BTreeMap<Vec<u8>, StatePointer>> =
            (0..subshard_count).map(|_| BTreeMap::new()).collect();
        for (key, pointer) in state {
            let subshard = checkpoint_subshard(&key, subshard_count)?;
            subgroups[subshard].insert(key, pointer);
        }
        let mut subshards = vec![ObjectRef::default(); subshard_count];
        for (subshard, entries) in subgroups.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let bytes = encode_checkpoint_partition(&entries)?;
            let reference = put_object(store, &bytes).await?;
            stats.subshard_objects += 1;
            stats.subshard_bytes += bytes.len() as u64;
            subshards[subshard] = reference;
        }
        if subshards.iter().any(|item| !item.is_empty()) {
            let bytes = encode_checkpoint_partition_manifest(&subshards)?;
            let reference = put_object(store, &bytes).await?;
            stats.partition_manifest_objects += 1;
            stats.partition_manifest_bytes += bytes.len() as u64;
            output_partitions[partition] = reference;
        }
    }
    let boundary = epochs
        .last()
        .context("checkpoint requires at least one epoch")?
        .end;
    let manifest = put_object(
        store,
        &encode_checkpoint_manifest(boundary, &output_partitions),
    )
    .await?;
    Ok((manifest, stats))
}

async fn pointer_value(
    store: &dyn ArchiveStore,
    pointer: StatePointer,
    cache: &mut CheckpointDataCache,
) -> Result<Vec<u8>> {
    if !cache.objects.contains_key(&pointer.object.digest) {
        let bytes = get_verified(store, pointer.object, MAX_DATA_OBJECT_BYTES).await?;
        let decoded_bytes = compressed_decoded_len(&bytes, MAX_DATA_DECODED)?;
        let data = decode_data(pointer.object, &bytes)?;
        while cache.objects.len() >= CHECKPOINT_BUILD_CACHE_MAX_ITEMS
            || cache.decoded_bytes + decoded_bytes > CHECKPOINT_BUILD_CACHE_MAX_BYTES
        {
            let digest = cache
                .order
                .pop_front()
                .context("checkpoint data cache accounting is inconsistent")?;
            let (_, evicted_bytes) = cache
                .objects
                .remove(&digest)
                .context("checkpoint data cache entry is missing")?;
            cache.decoded_bytes -= evicted_bytes;
        }
        cache.order.push_back(pointer.object.digest);
        cache.decoded_bytes += decoded_bytes;
        cache
            .objects
            .insert(pointer.object.digest, (data, decoded_bytes));
    }
    let data = &cache.objects[&pointer.object.digest].0;
    let (_, versions) = data
        .runs
        .get(pointer.run_index as usize)
        .context("checkpoint run pointer out of range")?;
    Ok(versions
        .get(pointer.version_index as usize)
        .context("checkpoint version pointer out of range")?
        .value
        .clone())
}

async fn append_catalog(
    store: &dyn ArchiveStore,
    current: ObjectRef,
    entry: CatalogEntry,
) -> Result<ObjectRef> {
    let mut root = if current.is_empty() {
        CatalogRoot { chunks: Vec::new() }
    } else {
        let bytes = get_verified(store, current, MAX_CATALOG_BYTES).await?;
        decode_catalog_root(current, &bytes)?
    };
    if let Some((_, last_ref)) = root.chunks.last_mut() {
        let bytes = get_verified(store, *last_ref, MAX_CATALOG_BYTES).await?;
        let mut chunk = decode_catalog_chunk(*last_ref, &bytes)?;
        if chunk.entries.len() < CATALOG_CHUNK_ENTRIES {
            chunk.entries.push(entry);
            *last_ref = put_object(store, &encode_catalog_chunk(&chunk)?).await?;
            return put_object(store, &encode_catalog_root(&root)?).await;
        }
    }
    if root.chunks.len() >= MAX_CATALOG_CHUNKS {
        bail!("completed-window catalog capacity exceeded");
    }
    let start = entry.start;
    let chunk = put_object(
        store,
        &encode_catalog_chunk(&CatalogChunk {
            entries: vec![entry],
        })?,
    )
    .await?;
    root.chunks.push((start, chunk));
    put_object(store, &encode_catalog_root(&root)?).await
}

fn is_default(key: &[u8], value: &[u8]) -> Result<bool> {
    match key.first().copied() {
        Some(NS_ACCOUNT) => Ok(value.first() == Some(&0)),
        Some(NS_STORAGE) => Ok(decode_storage(value)? == [0; 32]),
        Some(_) => Ok(false),
        None => bail!("empty state key"),
    }
}

async fn validate_code_references(
    prior: Option<&Publication>,
    changes: &BTreeMap<Vec<u8>, Vec<Version>>,
    accounts: &[AccountEvent],
) -> Result<()> {
    let empty = Hash32(Keccak256::digest([]).into());
    let mut checked = BTreeSet::new();
    for account in accounts {
        if !account.exists || account.code_hash == empty || !checked.insert(account.code_hash) {
            continue;
        }
        let key = full_key(NS_CODE, &account.code_hash.0);
        let local = changes.get(&key).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|version| version.block <= account.block)
                .map(|version| version.value.clone())
        });
        let value = match local {
            Some(value) => Some(value),
            None => match prior {
                Some(publication) => publication
                    .lookup_raw(&key, account.block.min(publication.commit.published_number))
                    .await?
                    .map(|(_, value)| value),
                None => None,
            },
        };
        let Some(value) = value else {
            bail!(
                "account references unavailable bytecode {}",
                account.code_hash
            );
        };
        let (first_seen, _) = decode_code_meta(&value)?;
        if first_seen > account.block {
            bail!("account references bytecode before first seen block");
        }
    }
    Ok(())
}

pub async fn load_head_commit(store: &dyn ArchiveStore, chain_id: u64) -> Result<(Head, Commit)> {
    let value = store
        .read_mutable_bounded(&head_key(chain_id), HEAD_BYTES)
        .await?
        .ok_or_else(|| anyhow!("archive has no published archive head"))?;
    load_from_head(store, &value, chain_id).await
}

async fn load_from_head(
    store: &dyn ArchiveStore,
    value: &VersionedBytes,
    chain_id: u64,
) -> Result<(Head, Commit)> {
    let head = Head::decode(&value.bytes)?;
    if head.chain_id != chain_id {
        bail!("archive head chain identity mismatch");
    }
    if head.commit.length as usize != COMMIT_BYTES {
        bail!("commit reference length is not fixed commit size");
    }
    let bytes = get_verified(store, head.commit, COMMIT_BYTES).await?;
    let commit = Commit::decode(&bytes)?;
    if commit.chain_id != chain_id
        || commit.generation != head.generation
        || commit.published_number != head.number
        || commit.published_hash != head.hash
    {
        bail!("archive commit does not match head");
    }
    Ok((head, commit))
}

fn validate_active_directory(commit: &Commit, directory: &Directory) -> Result<()> {
    if directory.window_start != commit.active_window_start {
        bail!("commit active window does not match active directory");
    }
    match directory.epochs.last() {
        Some(last) if last.end == commit.published_number => Ok(()),
        Some(_) => bail!("active directory is missing the committed latest epoch"),
        None => {
            let expected = commit
                .published_number
                .checked_add(1)
                .context("published block overflow for empty active directory")?;
            if commit.active_window_start != expected {
                bail!("empty active directory is not immediately after published head");
            }
            Ok(())
        }
    }
}

fn gate_fields(gate: &PublicationGate) -> (u64, bool) {
    match gate {
        PublicationGate::Finalized { number, .. } => (*number, true),
        PublicationGate::FixedOffset {
            observed_number,
            offset,
            ..
        } => (observed_number.saturating_sub(*offset), false),
    }
}

fn validate_transition(package: &Package, old: Option<&Commit>) -> Result<()> {
    match (package.mode, old) {
        (Mode::Anchor, None) => Ok(()),
        (Mode::Delta, None) => bail!("first publication must be an anchor package"),
        (Mode::Anchor, Some(_)) => bail!("canonical conflict: archive already has an anchor"),
        (Mode::Delta, Some(commit)) => {
            if commit.chain_id != package.segment.chain_id
                || commit.genesis_hash != package.segment.genesis_hash
            {
                bail!("package chain identity does not match archive");
            }
            if package.preceding_number != Some(commit.published_number)
                || package.preceding_hash != Some(commit.published_hash)
            {
                bail!("canonical gap/conflict at archive head");
            }
            Ok(())
        }
    }
}

fn validate_checkpoint(
    package: &Package,
    old: Option<&Commit>,
    gate: &PublicationGate,
) -> Result<()> {
    validate_checkpoint_segment(&package.segment.blocks, old, gate)
}

fn validate_checkpoint_segment(
    blocks: &[BlockMeta],
    old: Option<&Commit>,
    gate: &PublicationGate,
) -> Result<()> {
    let (number, hash) = match gate {
        PublicationGate::Finalized { number, hash } => (*number, *hash),
        PublicationGate::FixedOffset {
            observed_number,
            observed_hash,
            ..
        } => (*observed_number, *observed_hash),
    };
    let found = blocks
        .iter()
        .find(|block| block.number == number)
        .map(|block| block.hash)
        .or_else(|| {
            old.filter(|commit| commit.published_number == number)
                .map(|commit| commit.published_hash)
        });
    if found != Some(hash) {
        bail!("publication checkpoint hash is unavailable or not canonical");
    }
    Ok(())
}

fn truncate(mut segment: crate::format::Segment, eligible: u64) -> Result<crate::format::Segment> {
    segment.blocks.retain(|event| event.number <= eligible);
    segment.accounts.retain(|event| event.block <= eligible);
    segment.storage.retain(|event| event.block <= eligible);
    segment
        .code
        .retain(|event| event.first_seen_block <= eligible);
    segment.end_block = segment.blocks.last().context("empty publication")?.number;
    Ok(segment)
}

#[derive(Clone)]
pub struct Publication {
    pub head: Head,
    pub commit: Commit,
    store: Arc<dyn ArchiveStore>,
    cache: Cache<Hash32, Arc<Vec<u8>>>,
    inflight: Cache<Hash32, Arc<tokio::sync::Mutex<()>>>,
}

impl Publication {
    pub async fn load(store: Arc<dyn ArchiveStore>, chain_id: u64) -> Result<Self> {
        let (head, commit) = load_head_commit(store.as_ref(), chain_id).await?;
        Ok(Self::from_loaded(store, head, commit))
    }

    fn from_loaded(store: Arc<dyn ArchiveStore>, head: Head, commit: Commit) -> Self {
        let cache = Cache::builder()
            .max_capacity(64 * 1024 * 1024)
            .weigher(|_: &Hash32, value: &Arc<Vec<u8>>| value.len().min(u32::MAX as usize) as u32)
            .build();
        Self {
            head,
            commit,
            store,
            cache,
            inflight: Cache::builder().max_capacity(4_096).build(),
        }
    }

    async fn fetch(
        &self,
        reference: ObjectRef,
        maximum: usize,
        budget: &mut LookupBudget,
    ) -> Result<Arc<Vec<u8>>> {
        if reference.length as usize > maximum {
            bail!("archive object reference length exceeds pre-download bound");
        }
        if let Some(bytes) = self.cache.get(&reference.digest) {
            verify_object(reference, &bytes)?;
            return Ok(bytes);
        }
        let singleflight = self
            .inflight
            .get_with(reference.digest, || Arc::new(tokio::sync::Mutex::new(())));
        let _guard = singleflight.lock().await;
        if let Some(bytes) = self.cache.get(&reference.digest) {
            verify_object(reference, &bytes)?;
            return Ok(bytes);
        }
        budget.remote_get()?;
        let bytes = Arc::new(get_verified(self.store.as_ref(), reference, maximum).await?);
        self.cache.insert(reference.digest, bytes.clone());
        Ok(bytes)
    }

    async fn directory_at(
        &self,
        block: u64,
        budget: &mut LookupBudget,
    ) -> Result<(ObjectRef, Directory)> {
        if block >= self.commit.active_window_start {
            let reference = self.commit.active_directory;
            let bytes = self.fetch(reference, MAX_DIRECTORY_BYTES, budget).await?;
            budget.decoded(bytes.len())?;
            let directory = decode_directory(reference, &bytes)?;
            validate_active_directory(&self.commit, &directory)?;
            return Ok((reference, directory));
        }
        if self.commit.completed_catalog.is_empty() {
            bail!("block unavailable");
        }
        let root_ref = self.commit.completed_catalog;
        let bytes = self.fetch(root_ref, MAX_CATALOG_BYTES, budget).await?;
        budget.decoded(bytes.len())?;
        let root = decode_catalog_root(root_ref, &bytes)?;
        let position = root.chunks.partition_point(|(start, _)| *start <= block);
        let Some(index) = position.checked_sub(1) else {
            bail!("block unavailable");
        };
        let chunk_ref = root.chunks[index].1;
        let bytes = self.fetch(chunk_ref, MAX_CATALOG_BYTES, budget).await?;
        budget.decoded(bytes.len())?;
        let chunk = decode_catalog_chunk(chunk_ref, &bytes)?;
        let entry = chunk
            .entries
            .iter()
            .find(|entry| entry.start <= block && block <= entry.end)
            .context("block unavailable")?;
        let bytes = self
            .fetch(entry.directory, MAX_DIRECTORY_BYTES, budget)
            .await?;
        budget.decoded(bytes.len())?;
        let directory = decode_directory(entry.directory, &bytes)?;
        let actual_start = directory.window_start;
        let actual_end = directory
            .epochs
            .last()
            .context("completed directory has no epochs")?
            .end;
        if entry.start != actual_start || entry.end != actual_end {
            bail!("catalog entry range does not match completed directory");
        }
        Ok((entry.directory, directory))
    }

    async fn lookup_raw(&self, key: &[u8], block: u64) -> Result<Option<(u64, Vec<u8>)>> {
        let mut budget = LookupBudget::default();
        self.lookup_raw_budget(key, block, &mut budget).await
    }

    async fn lookup_raw_budget(
        &self,
        key: &[u8],
        block: u64,
        budget: &mut LookupBudget,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        self.require_block(block)?;
        let (_, directory) = self.directory_at(block, budget).await?;
        let route = route_key(key)?;
        for epoch in directory.epochs.iter().rev() {
            if epoch.start > block {
                continue;
            }
            let index_ref = epoch.indexes[route.partition as usize];
            if index_ref.is_empty() {
                continue;
            }
            let bytes = self.fetch(index_ref, MAX_INDEX_BYTES, budget).await?;
            budget.decoded(bytes.len())?;
            let index = decode_index(index_ref, &bytes)?;
            if index.partition != route.partition || index.epoch_start != epoch.start {
                bail!("index partition or epoch mismatch");
            }
            for entry in index.entries.iter().filter(|entry| {
                entry.fingerprint == route.fingerprint && entry.first_block <= block
            }) {
                let data_ref = index.dictionary[entry.dictionary_index as usize];
                let expected_data = epoch.data[(route.partition >> 2) as usize];
                if data_ref != expected_data {
                    bail!("index dictionary data reference does not match epoch descriptor");
                }
                let bytes = self.fetch(data_ref, MAX_DATA_OBJECT_BYTES, budget).await?;
                budget.decoded(compressed_decoded_len(&bytes, MAX_DATA_DECODED)?)?;
                let data = decode_data(data_ref, &bytes)?;
                let Some((candidate_key, versions)) = data.runs.get(entry.run_index as usize)
                else {
                    bail!("index run offset out of range");
                };
                if candidate_key != key {
                    continue;
                }
                if let Some(version) = versions.iter().rev().find(|version| version.block <= block)
                {
                    return Ok(Some((version.block, version.value.clone())));
                }
            }
        }
        let checkpoint = directory.base_checkpoint;
        if checkpoint.is_empty() {
            return Ok(None);
        }
        let bytes = self
            .fetch(checkpoint, CHECKPOINT_MANIFEST_BYTES, budget)
            .await?;
        budget.decoded(bytes.len())?;
        let manifest = decode_checkpoint_manifest(checkpoint, &bytes)?;
        let expected_boundary = directory
            .window_start
            .checked_sub(1)
            .context("nonempty base checkpoint at block zero")?;
        if manifest.boundary != expected_boundary {
            bail!("checkpoint boundary does not match window directory");
        }
        let partition_manifest = manifest.partitions[route.partition as usize];
        if partition_manifest.is_empty() {
            return Ok(None);
        }
        let bytes = self
            .fetch(
                partition_manifest,
                MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES,
                budget,
            )
            .await?;
        budget.decoded(bytes.len())?;
        let subshards = decode_checkpoint_partition_manifest(partition_manifest, &bytes)?;
        let subshard = subshards[checkpoint_subshard(key, subshards.len())?];
        if subshard.is_empty() {
            return Ok(None);
        }
        let bytes = self
            .fetch(subshard, MAX_CHECKPOINT_OBJECT_BYTES, budget)
            .await?;
        budget.decoded(compressed_decoded_len(
            &bytes,
            MAX_CHECKPOINT_SHARD_DECODED,
        )?)?;
        let entries = decode_checkpoint_partition(subshard, &bytes)?;
        let Some(pointer) = entries.get(key) else {
            return Ok(None);
        };
        let bytes = self
            .fetch(pointer.object, MAX_DATA_OBJECT_BYTES, budget)
            .await?;
        budget.decoded(compressed_decoded_len(&bytes, MAX_DATA_DECODED)?)?;
        let data = decode_data(pointer.object, &bytes)?;
        let Some((candidate_key, versions)) = data.runs.get(pointer.run_index as usize) else {
            bail!("checkpoint run offset out of range");
        };
        if candidate_key != key {
            bail!("checkpoint pointer full-key mismatch");
        }
        let version = checkpoint_version(versions, pointer.version_index, manifest.boundary)?;
        Ok(Some((version.block, version.value.clone())))
    }

    async fn account_at_budget(
        &self,
        address: Address,
        block: u64,
        budget: &mut LookupBudget,
    ) -> Result<Option<AccountEvent>> {
        let key = full_key(NS_ACCOUNT, &address.0);
        let Some((changed, value)) = self.lookup_raw_budget(&key, block, budget).await? else {
            return Ok(None);
        };
        let account = decode_account(changed, address, &value)?;
        Ok(account.exists.then_some(account))
    }

    pub async fn account_at(&self, address: Address, block: u64) -> Result<Option<AccountEvent>> {
        let mut budget = LookupBudget::default();
        self.account_at_budget(address, block, &mut budget).await
    }

    pub async fn storage_at(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]> {
        let mut budget = LookupBudget::default();
        let Some(account) = self.account_at_budget(address, block, &mut budget).await? else {
            return Ok([0; 32]);
        };
        let mut logical = address.0.to_vec();
        logical.extend_from_slice(&account.incarnation.to_be_bytes());
        logical.extend_from_slice(&slot.0);
        let key = full_key(NS_STORAGE, &logical);
        match self.lookup_raw_budget(&key, block, &mut budget).await? {
            Some((_, value)) => decode_storage(&value),
            None => Ok([0; 32]),
        }
    }

    pub async fn code_at(&self, address: Address, block: u64) -> Result<Vec<u8>> {
        let mut budget = LookupBudget::default();
        let Some(account) = self.account_at_budget(address, block, &mut budget).await? else {
            return Ok(Vec::new());
        };
        let key = full_key(NS_CODE, &account.code_hash.0);
        let Some((_, value)) = self.lookup_raw_budget(&key, block, &mut budget).await? else {
            return Ok(Vec::new());
        };
        let (first_seen, object) = decode_code_meta(&value)?;
        if first_seen > block {
            return Ok(Vec::new());
        }
        let bytes = self
            .fetch(object, MAX_CODE_OBJECT_BYTES, &mut budget)
            .await?;
        budget.decoded(bytes.len())?;
        if Hash32(Keccak256::digest(bytes.as_slice()).into()) != account.code_hash {
            bail!("code Keccak mismatch");
        }
        Ok(bytes.as_ref().clone())
    }

    pub async fn block_number_by_hash(&self, _hash: Hash32) -> Result<Option<u64>> {
        bail!("block-hash selectors are unsupported until a bounded durable hash index exists")
    }

    pub fn require_block(&self, block: u64) -> Result<()> {
        if block < self.commit.anchor_number || block > self.commit.published_number {
            bail!("block unavailable");
        }
        Ok(())
    }
}

pub fn benchmark_forced_collision_check() -> Result<bool> {
    let key_a = full_key(NS_ACCOUNT, &[1; 20]);
    let key_b = full_key(NS_ACCOUNT, &[2; 20]);
    let mut runs = BTreeMap::new();
    runs.insert(
        key_a,
        vec![Version {
            block: 1,
            value: b"a".to_vec(),
        }],
    );
    runs.insert(
        key_b.clone(),
        vec![Version {
            block: 1,
            value: b"b".to_vec(),
        }],
    );
    let bytes = encode_data(&runs)?;
    let reference = ObjectRef {
        digest: Hash32::digest(&bytes),
        length: bytes.len().try_into()?,
    };
    let data = decode_data(reference, &bytes)?;
    // Both candidates deliberately share the same synthetic fingerprint. Only the
    // exact full-key comparison may select the requested run.
    let candidates = [0_usize, 1_usize];
    Ok(candidates.into_iter().find_map(|run| {
        let (key, versions) = data.runs.get(run)?;
        (key == &key_b).then(|| versions[0].value.as_slice())
    }) == Some(b"b".as_slice()))
}

fn checkpoint_version(versions: &[Version], version_index: u32, boundary: u64) -> Result<&Version> {
    let index = version_index as usize;
    let version = versions
        .get(index)
        .context("checkpoint version offset out of range")?;
    if version.block > boundary
        || versions
            .get(index + 1)
            .is_some_and(|next| next.block <= boundary)
    {
        bail!("checkpoint pointer is not latest at checkpoint boundary");
    }
    Ok(version)
}

fn encode_account(event: &AccountEvent) -> Vec<u8> {
    let mut out = vec![u8::from(event.exists)];
    put_uleb(&mut out, event.incarnation);
    put_uleb(&mut out, event.nonce);
    put_trimmed_u256(&mut out, &event.balance);
    out.extend_from_slice(&event.code_hash.0);
    out
}

fn decode_account(block: u64, address: Address, bytes: &[u8]) -> Result<AccountEvent> {
    let mut input = Cursor::new(bytes);
    let exists = match input.byte()? {
        0 => false,
        1 => true,
        _ => bail!("noncanonical account existence flag"),
    };
    let incarnation = input.uleb()?;
    let nonce = input.uleb()?;
    let balance = input.trimmed_u256()?;
    let code_hash = input.hash()?;
    input.end()?;
    Ok(AccountEvent {
        block,
        address,
        exists,
        incarnation,
        nonce,
        balance,
        code_hash,
    })
}

fn encode_storage(event: &StorageEvent) -> Vec<u8> {
    let mut out = Vec::new();
    put_trimmed_u256(&mut out, &event.value);
    out
}

fn decode_storage(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut input = Cursor::new(bytes);
    let value = input.trimmed_u256()?;
    input.end()?;
    Ok(value)
}

fn encode_code_meta(first_seen: u64, object: ObjectRef) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, first_seen);
    put_ref(&mut out, object);
    out
}

fn decode_code_meta(bytes: &[u8]) -> Result<(u64, ObjectRef)> {
    let mut input = Cursor::new(bytes);
    let first_seen = input.u64()?;
    let object = input.object_ref()?;
    validate_ref(object)?;
    if object.length as usize > MAX_CODE_OBJECT_BYTES {
        bail!("code object reference exceeds 1 MiB prototype limit");
    }
    input.end()?;
    Ok((first_seen, object))
}

fn verify_object(expected: ObjectRef, bytes: &[u8]) -> Result<()> {
    if bytes.len() != expected.length as usize || Hash32::digest(bytes) != expected.digest {
        bail!("archive object length or digest mismatch");
    }
    Ok(())
}

fn validate_ref(reference: ObjectRef) -> Result<()> {
    if reference.length == 0 || reference.digest == Hash32([0; 32]) {
        bail!("invalid archive object reference");
    }
    Ok(())
}

fn validate_optional_ref(reference: ObjectRef) -> Result<()> {
    if reference.is_empty() {
        Ok(())
    } else {
        validate_ref(reference)
    }
}

fn bounded_count(input: &mut Cursor<'_>, minimum: usize, label: &str) -> Result<usize> {
    let count = input.uleb()?;
    if count > (input.remaining() / minimum) as u64 {
        bail!("{label} count exceeds decoded length");
    }
    count.try_into().context("entry count exceeds usize")
}

fn put_ref(out: &mut Vec<u8>, reference: ObjectRef) {
    out.extend_from_slice(&reference.digest.0);
    put_u32(out, reference.length);
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_uleb(out, value.len() as u64);
    out.extend_from_slice(value);
}
fn put_uleb(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}
fn put_trimmed_u256(out: &mut Vec<u8>, value: &[u8; 32]) {
    let first = value.iter().position(|byte| *byte != 0).unwrap_or(32);
    out.push((32 - first) as u8);
    out.extend_from_slice(&value[first..]);
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .context("length overflow")?;
        if end > self.bytes.len() {
            bail!("truncated archive data");
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }
    fn expect(&mut self, value: &[u8]) -> Result<()> {
        if self.take(value.len())? != value {
            bail!("archive magic mismatch");
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn hash(&mut self) -> Result<Hash32> {
        Ok(Hash32(self.take(32)?.try_into().unwrap()))
    }
    fn object_ref(&mut self) -> Result<ObjectRef> {
        Ok(ObjectRef {
            digest: self.hash()?,
            length: self.u32()?,
        })
    }
    fn uleb(&mut self) -> Result<u64> {
        let start = self.position;
        let mut value = 0_u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                bail!("ULEB128 overflow");
            }
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                let mut canonical = Vec::new();
                put_uleb(&mut canonical, value);
                if self.bytes[start..self.position] != canonical {
                    bail!("noncanonical ULEB128");
                }
                return Ok(value);
            }
        }
        bail!("ULEB128 overflow")
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length: usize = self.uleb()?.try_into().context("byte string too large")?;
        self.take(length)
    }
    fn trimmed_u256(&mut self) -> Result<[u8; 32]> {
        let length = self.byte()? as usize;
        if length > 32 {
            bail!("invalid U256 length");
        }
        let bytes = self.take(length)?;
        if bytes.first() == Some(&0) {
            bail!("noncanonical U256");
        }
        let mut value = [0; 32];
        value[32 - length..].copy_from_slice(bytes);
        Ok(value)
    }
    fn end(&self) -> Result<()> {
        if self.remaining() != 0 {
            bail!("trailing archive data");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Segment, SEGMENT_SCHEMA};
    use crate::store::MemoryArchiveStore;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn hash(number: u64) -> Hash32 {
        Hash32::digest(&number.to_be_bytes())
    }

    fn package(block: u64, anchor: bool, with_account: bool) -> Package {
        let empty_code = Hash32(Keccak256::digest([]).into());
        let accounts = if with_account {
            vec![AccountEvent {
                block,
                address: Address([7; 20]),
                exists: true,
                incarnation: 0,
                nonce: block,
                balance: {
                    let mut value = [0; 32];
                    value[24..].copy_from_slice(&(block + 1).to_be_bytes());
                    value
                },
                code_hash: empty_code,
            }]
        } else {
            Vec::new()
        };
        Package {
            mode: if anchor { Mode::Anchor } else { Mode::Delta },
            preceding_number: (!anchor).then(|| block - 1),
            preceding_hash: (!anchor).then(|| hash(block - 1)),
            input_sha256: Hash32::digest(&[block as u8, u8::from(anchor)]),
            segment: Segment {
                schema: SEGMENT_SCHEMA.to_owned(),
                chain_id: 1,
                genesis_hash: hash(0),
                start_block: block,
                end_block: block,
                blocks: vec![BlockMeta {
                    number: block,
                    hash: hash(block),
                    parent_hash: if block == 0 {
                        Hash32([0; 32])
                    } else {
                        hash(block - 1)
                    },
                    state_root: Hash32([block as u8; 32]),
                    timestamp: block,
                }],
                accounts,
                storage: Vec::new(),
                code: Vec::new(),
            },
        }
    }

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryArchiveStore,
        reads: AtomicU64,
    }

    #[async_trait]
    impl ArchiveStore for CountingStore {
        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key).await
        }
        async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_bounded(key, maximum).await
        }
        async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
            self.inner.put_immutable(key, bytes).await
        }
        async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_mutable(key).await
        }
        async fn read_mutable_bounded(
            &self,
            key: &str,
            maximum: usize,
        ) -> Result<Option<VersionedBytes>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_mutable_bounded(key, maximum).await
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

    #[test]
    fn format_v1_golden_contract_pins_magics_head_key_and_head_vector() {
        assert_eq!(VERSION, 1);
        assert_eq!(HEAD_MAGIC, b"FSEH");
        assert_eq!(COMMIT_MAGIC, b"FSEC");
        assert_eq!(DATA_MAGIC, b"FSED");
        assert_eq!(INDEX_MAGIC, b"FSEI");
        assert_eq!(BLOCK_MAGIC, b"FSEB");
        assert_eq!(DIRECTORY_MAGIC, b"FSER");
        assert_eq!(CHECKPOINT_MAGIC, b"FSEP");
        assert_eq!(CHECKPOINT_MANIFEST_MAGIC, b"FSEM");
        assert_eq!(CATALOG_ROOT_MAGIC, b"FSEW");
        assert_eq!(CATALOG_CHUNK_MAGIC, b"FSWC");
        assert_eq!(CHECKPOINT_PARTITION_MAGIC, b"FSPS");
        assert_eq!(head_key(8_453), "chains/0x2105/heads/finalized.bin");

        let head = Head {
            generation: 1,
            chain_id: 8_453,
            number: 42,
            hash: Hash32([0x11; 32]),
            commit: ObjectRef {
                digest: Hash32([0x22; 32]),
                length: COMMIT_BYTES as u32,
            },
        };
        let expected = hex::decode(concat!(
            "4653454801",
            "0000000000000001",
            "0000000000002105",
            "000000000000002a",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "0000013a"
        ))
        .unwrap();
        assert_eq!(expected.len(), HEAD_BYTES);
        assert_eq!(expected[4], 1);
        assert_eq!(head.encode(), expected);
        assert_eq!(Head::decode(&expected).unwrap(), head);

        let partition_manifest =
            encode_checkpoint_partition_manifest(&[ObjectRef::default(); 8]).unwrap();
        assert_eq!(&partition_manifest[..7], b"FSPS\x01\x00\x08");
        assert_eq!(partition_manifest.len(), 4 + 1 + 2 + 8 * REF_BYTES);
        let partition_ref = ObjectRef {
            digest: Hash32::digest(&partition_manifest),
            length: partition_manifest.len() as u32,
        };
        assert_eq!(
            decode_checkpoint_partition_manifest(partition_ref, &partition_manifest)
                .unwrap()
                .len(),
            8
        );

        let mut legacy_partition_manifest = b"FSPS\x01".to_vec();
        for _ in 0..8 {
            put_ref(&mut legacy_partition_manifest, ObjectRef::default());
        }
        assert_eq!(
            legacy_partition_manifest.len(),
            LEGACY_CHECKPOINT_PARTITION_MANIFEST_BYTES
        );
        assert_eq!(legacy_partition_manifest.len(), 293);
        let legacy_ref = ObjectRef {
            digest: Hash32::digest(&legacy_partition_manifest),
            length: legacy_partition_manifest.len() as u32,
        };
        assert_eq!(
            decode_checkpoint_partition_manifest(legacy_ref, &legacy_partition_manifest)
                .unwrap()
                .len(),
            8
        );
    }

    #[test]
    fn synthetic_oversized_checkpoint_partition_forces_8_to_16_and_rejects_bad_counts() {
        let pointer = StatePointer {
            object: ObjectRef {
                digest: Hash32([1; 32]),
                length: 1,
            },
            run_index: 0,
            version_index: 0,
        };
        let mut first_by_route: BTreeMap<(u8, usize), (Vec<u8>, usize)> = BTreeMap::new();
        let mut selected = None;
        for value in 0u32..100_000 {
            let mut address = [0u8; 20];
            address[16..].copy_from_slice(&value.to_be_bytes());
            let key = full_key(NS_ACCOUNT, &address);
            let route = route_key(&key).unwrap();
            let shard8 = checkpoint_subshard(&key, 8).unwrap();
            let shard16 = checkpoint_subshard(&key, 16).unwrap();
            if let Some((other, other16)) = first_by_route.get(&(route.partition, shard8)) {
                if *other16 != shard16 {
                    selected = Some((other.clone(), key));
                    break;
                }
            } else {
                first_by_route.insert((route.partition, shard8), (key, shard16));
            }
        }
        let (left, right) = selected.expect("find keys that split on the fourth hash bit");
        assert_eq!(
            route_key(&left).unwrap().partition,
            route_key(&right).unwrap().partition
        );
        assert_eq!(
            checkpoint_subshard(&left, 8).unwrap(),
            checkpoint_subshard(&right, 8).unwrap()
        );
        assert_ne!(
            checkpoint_subshard(&left, 16).unwrap(),
            checkpoint_subshard(&right, 16).unwrap()
        );
        let mut state = BTreeMap::new();
        state.insert(left.clone(), pointer);
        state.insert(right, pointer);
        // Scale the production byte target down to one entry: the exact decoded-body
        // sizing path sees the 8-way shard as oversized and the 16-way split as valid.
        let one_entry_target = checkpoint_entry_decoded_len(&left).unwrap() + 1;
        assert_eq!(
            select_checkpoint_subshard_count(&state, one_entry_target).unwrap(),
            16
        );
        assert!(select_checkpoint_subshard_count(&state, 1)
            .unwrap_err()
            .to_string()
            .contains("capacity exceeded"));

        let mut malformed = encode_checkpoint_partition_manifest(&vec![
            ObjectRef::default();
            MIN_CHECKPOINT_SUBSHARDS
        ])
        .unwrap();
        malformed[5..7].copy_from_slice(&12u16.to_be_bytes());
        let reference = ObjectRef {
            digest: Hash32::digest(&malformed),
            length: malformed.len() as u32,
        };
        let malformed_error =
            decode_checkpoint_partition_manifest(reference, &malformed).unwrap_err();
        assert!(!malformed_error.to_string().contains("exceeds bound"));
        let oversized = vec![0; MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES + 1];
        let oversized_ref = ObjectRef {
            digest: Hash32::digest(&oversized),
            length: oversized.len() as u32,
        };
        assert!(
            decode_checkpoint_partition_manifest(oversized_ref, &oversized)
                .unwrap_err()
                .to_string()
                .contains("exceeds bound")
        );
        assert!(validate_checkpoint_subshard_count(4).is_err());
        assert!(validate_checkpoint_subshard_count(512).is_err());
    }

    #[test]
    fn native_publication_enforces_shared_worker_data_and_index_caps() {
        assert_eq!(MAX_DATA_DECODED, 8 * 1024 * 1024);
        assert_eq!(MAX_DATA_OBJECT_BYTES, MAX_DATA_DECODED + 256 * 1024);
        assert_eq!(MAX_INDEX_BYTES, 2 * 1024 * 1024);

        let mut runs = BTreeMap::new();
        runs.insert(
            full_key(NS_ACCOUNT, &[1; 20]),
            vec![Version {
                block: 1,
                value: vec![0; MAX_DATA_DECODED],
            }],
        );
        assert!(encode_data(&runs)
            .unwrap_err()
            .to_string()
            .contains("Worker 8 MiB decoded publication cap"));

        let reference = ObjectRef {
            digest: Hash32([1; 32]),
            length: 1,
        };
        let entry = IndexEntry {
            fingerprint: [0; 12],
            first_block: 0,
            dictionary_index: 0,
            run_index: 0,
        };
        let index = IndexDelta {
            partition: 0,
            epoch_start: 0,
            dictionary: vec![reference],
            entries: vec![entry; MAX_INDEX_ENTRIES + 1],
        };
        assert!(encode_index(&index)
            .unwrap_err()
            .to_string()
            .contains("65,536-entry publication cap"));
    }

    #[tokio::test]
    async fn checkpoint_builder_decoded_data_cache_evicts_per_partition() {
        let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        let mut pointers = Vec::new();
        for value in 1..=3u8 {
            let key = full_key(NS_ACCOUNT, &[value; 20]);
            let mut runs = BTreeMap::new();
            runs.insert(
                key,
                vec![Version {
                    block: 1,
                    value: vec![value],
                }],
            );
            let bytes = encode_data(&runs).unwrap();
            let reference = put_object(store.as_ref(), &bytes).await.unwrap();
            pointers.push(StatePointer {
                object: reference,
                run_index: 0,
                version_index: 0,
            });
        }
        let mut cache = CheckpointDataCache::default();
        for (expected, pointer) in (1..=3u8).zip(pointers.iter().copied()) {
            assert_eq!(
                pointer_value(store.as_ref(), pointer, &mut cache)
                    .await
                    .unwrap(),
                vec![expected]
            );
            assert!(cache.objects.len() <= CHECKPOINT_BUILD_CACHE_MAX_ITEMS);
            assert!(cache.decoded_bytes <= CHECKPOINT_BUILD_CACHE_MAX_BYTES);
        }
        assert_eq!(cache.objects.len(), CHECKPOINT_BUILD_CACHE_MAX_ITEMS);
        assert!(!cache.objects.contains_key(&pointers[0].object.digest));
    }

    #[test]
    fn commit_is_fixed_and_rejects_invalid_parent() {
        let commit = Commit {
            generation: 1,
            chain_id: 1,
            genesis_hash: Hash32([1; 32]),
            anchor_number: 0,
            anchor_hash: Hash32([1; 32]),
            published_hash: Hash32([1; 32]),
            published_number: 0,
            published_state_root: Hash32([2; 32]),
            parent: ObjectRef::default(),
            finalized: true,
            input_sha256: Hash32([3; 32]),
            active_window_start: 0,
            active_directory: ObjectRef {
                digest: Hash32([4; 32]),
                length: 1,
            },
            completed_catalog: ObjectRef::default(),
        };
        let bytes = commit.encode();
        assert_eq!(bytes.len(), COMMIT_BYTES);
        assert_eq!(Commit::decode(&bytes).unwrap(), commit);
        let mut invalid = commit;
        invalid.parent = ObjectRef {
            digest: Hash32([9; 32]),
            length: 0,
        };
        assert!(Commit::decode(&invalid.encode()).is_err());
    }

    #[tokio::test]
    async fn fingerprint_collision_verifies_full_key() {
        let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        let key_a = full_key(NS_ACCOUNT, &[1; 20]);
        let key_b = full_key(NS_ACCOUNT, &[2; 20]);
        let mut runs = BTreeMap::new();
        runs.insert(
            key_a.clone(),
            vec![Version {
                block: 1,
                value: b"a".to_vec(),
            }],
        );
        runs.insert(
            key_b.clone(),
            vec![Version {
                block: 1,
                value: b"b".to_vec(),
            }],
        );
        let data_ref = put_object(store.as_ref(), &encode_data(&runs).unwrap())
            .await
            .unwrap();
        let fingerprint = [7; 12];
        let index = IndexDelta {
            partition: 0,
            epoch_start: 1,
            dictionary: vec![data_ref],
            entries: vec![
                IndexEntry {
                    fingerprint,
                    first_block: 1,
                    dictionary_index: 0,
                    run_index: 0,
                },
                IndexEntry {
                    fingerprint,
                    first_block: 1,
                    dictionary_index: 0,
                    run_index: 1,
                },
            ],
        };
        let data_bytes = get_verified(store.as_ref(), data_ref, MAX_DATA_OBJECT_BYTES)
            .await
            .unwrap();
        let data = decode_data(data_ref, &data_bytes).unwrap();
        let found = index.entries.iter().find_map(|entry| {
            let (key, versions) = data.runs.get(entry.run_index as usize)?;
            (key == &key_b).then(|| versions[0].value.clone())
        });
        assert_eq!(found, Some(b"b".to_vec()));
    }

    #[tokio::test]
    async fn malicious_index_cannot_redirect_outside_epoch_descriptor() {
        let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        let address = Address([4; 20]);
        let key = full_key(NS_ACCOUNT, &address.0);
        let account_value = encode_account(&AccountEvent {
            block: 1,
            address,
            exists: true,
            incarnation: 0,
            nonce: 1,
            balance: [1; 32],
            code_hash: Hash32(Keccak256::digest([]).into()),
        });
        let mut expected_runs = BTreeMap::new();
        expected_runs.insert(
            key.clone(),
            vec![Version {
                block: 1,
                value: account_value.clone(),
            }],
        );
        let expected_data = put_object(store.as_ref(), &encode_data(&expected_runs).unwrap())
            .await
            .unwrap();
        let mut malicious_runs = BTreeMap::new();
        let mut malicious_value = account_value;
        malicious_value[2] = 9;
        malicious_runs.insert(
            key.clone(),
            vec![Version {
                block: 1,
                value: malicious_value,
            }],
        );
        let malicious_data = put_object(store.as_ref(), &encode_data(&malicious_runs).unwrap())
            .await
            .unwrap();
        let route = route_key(&key).unwrap();
        let index = IndexDelta {
            partition: route.partition,
            epoch_start: 1,
            dictionary: vec![malicious_data],
            entries: vec![IndexEntry {
                fingerprint: route.fingerprint,
                first_block: 1,
                dictionary_index: 0,
                run_index: 0,
            }],
        };
        let index_ref = put_object(store.as_ref(), &encode_index(&index).unwrap())
            .await
            .unwrap();
        let mut data = [ObjectRef::default(); DATA_PARTITIONS];
        data[(route.partition >> 2) as usize] = expected_data;
        let mut indexes = [ObjectRef::default(); ROUTER_PARTITIONS];
        indexes[route.partition as usize] = index_ref;
        let directory = Directory {
            window_start: 1,
            base_checkpoint: ObjectRef::default(),
            epochs: vec![EpochDescriptor {
                start: 1,
                end: 1,
                blocks: ObjectRef {
                    digest: Hash32([8; 32]),
                    length: 1,
                },
                data,
                indexes,
            }],
        };
        let directory_ref = put_object(store.as_ref(), &encode_directory(&directory).unwrap())
            .await
            .unwrap();
        let commit = Commit {
            generation: 1,
            chain_id: 1,
            genesis_hash: hash(0),
            anchor_number: 1,
            anchor_hash: hash(1),
            published_hash: hash(1),
            published_number: 1,
            published_state_root: Hash32([0; 32]),
            parent: ObjectRef::default(),
            finalized: true,
            input_sha256: Hash32([0; 32]),
            active_window_start: 1,
            active_directory: directory_ref,
            completed_catalog: ObjectRef::default(),
        };
        let publication = Publication::from_loaded(
            store,
            Head {
                generation: 1,
                chain_id: 1,
                number: 1,
                hash: hash(1),
                commit: ObjectRef {
                    digest: Hash32([7; 32]),
                    length: COMMIT_BYTES as u32,
                },
            },
            commit,
        );
        let error = publication.account_at(address, 1).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match epoch descriptor"));
    }

    #[tokio::test]
    async fn rollover_checkpoint_resolves_value_unchanged_for_millions_of_blocks() {
        let store = Arc::new(CountingStore::default());
        let mut anchor = package(0, true, true);
        let mut unchanged_account = anchor.segment.accounts[0].clone();
        unchanged_account.address = Address([6; 20]);
        unchanged_account.balance[31] = 2;
        anchor.segment.accounts.push(unchanged_account);
        let slot = Hash32([9; 32]);
        anchor.segment.storage.push(StorageEvent {
            block: 0,
            address: Address([7; 20]),
            incarnation: 0,
            slot,
            value: [5; 32],
        });
        let first = publish(
            store.clone(),
            anchor,
            PublicationGate::Finalized {
                number: 0,
                hash: hash(0),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.generation, 1);
        for block in 1..63 {
            publish(
                store.clone(),
                package(block, false, false),
                PublicationGate::Finalized {
                    number: block,
                    hash: hash(block),
                },
            )
            .await
            .unwrap();
        }
        let mut next = package(63, false, false);
        next.segment.end_block = 65;
        for block in [64_u64, 65] {
            next.segment.blocks.push(BlockMeta {
                number: block,
                hash: hash(block),
                parent_hash: hash(block - 1),
                state_root: Hash32([block as u8; 32]),
                timestamp: block,
            });
        }
        let mut tombstone = package(64, false, true).segment.accounts.remove(0);
        tombstone.exists = false;
        tombstone.balance = [0; 32];
        let mut recreated = package(65, false, true).segment.accounts.remove(0);
        recreated.incarnation = 1;
        recreated.balance[31] = 9;
        next.segment.accounts.extend([tombstone, recreated]);
        next.input_sha256 = Hash32::digest(b"blocks-63-65");
        publish(
            store.clone(),
            next,
            PublicationGate::Finalized {
                number: 65,
                hash: hash(65),
            },
        )
        .await
        .unwrap();

        let publication = Publication::load(store.clone(), 1).await.unwrap();
        assert_eq!(publication.commit.active_window_start, 66);
        let mut old_budget = LookupBudget::default();
        publication.directory_at(0, &mut old_budget).await.unwrap();
        assert_eq!(old_budget.remote_gets, 3);
        let mut missing = Publication::load(store.clone(), 1).await.unwrap();
        missing.commit.active_window_start = 128;
        let mut missing_budget = LookupBudget::default();
        assert!(missing
            .directory_at(100, &mut missing_budget)
            .await
            .is_err());
        assert!(missing_budget.remote_gets <= 2);
        assert_eq!(publication.commit.active_window_start, 66);
        assert!(!publication.commit.completed_catalog.is_empty());
        let at_zero = publication
            .account_at(Address([7; 20]), 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_zero.balance[31], 1);
        let before_first_change = publication
            .account_at(Address([7; 20]), 63)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before_first_change.balance[31], 1);
        assert!(publication
            .account_at(Address([7; 20]), 64)
            .await
            .unwrap()
            .is_none());
        let after_change = publication
            .account_at(Address([7; 20]), 65)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_change.balance[31], 9);
        assert_eq!(
            publication
                .storage_at(Address([7; 20]), slot, 0)
                .await
                .unwrap(),
            [5; 32]
        );
        assert_eq!(
            publication
                .storage_at(Address([7; 20]), slot, 65)
                .await
                .unwrap(),
            [0; 32]
        );

        let active_bytes = get_verified(
            store.as_ref(),
            publication.commit.active_directory,
            MAX_DIRECTORY_BYTES,
        )
        .await
        .unwrap();
        let active = decode_directory(publication.commit.active_directory, &active_bytes).unwrap();
        let manifest_bytes = get_verified(
            store.as_ref(),
            active.base_checkpoint,
            CHECKPOINT_MANIFEST_BYTES,
        )
        .await
        .unwrap();
        let manifest = decode_checkpoint_manifest(active.base_checkpoint, &manifest_bytes).unwrap();
        let mut legacy_partitions = [ObjectRef::default(); ROUTER_PARTITIONS];
        for (partition, reference) in manifest.partitions.iter().copied().enumerate() {
            if reference.is_empty() {
                continue;
            }
            let bytes = get_verified(
                store.as_ref(),
                reference,
                MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES,
            )
            .await
            .unwrap();
            let subshards = decode_checkpoint_partition_manifest(reference, &bytes).unwrap();
            assert_eq!(subshards.len(), MIN_CHECKPOINT_SUBSHARDS);
            let mut legacy = b"FSPS\x01".to_vec();
            for subshard in subshards {
                put_ref(&mut legacy, subshard);
            }
            assert_eq!(legacy.len(), LEGACY_CHECKPOINT_PARTITION_MANIFEST_BYTES);
            legacy_partitions[partition] = put_object(store.as_ref(), &legacy).await.unwrap();
        }
        let mut storage_logical = Address([7; 20]).0.to_vec();
        storage_logical.extend_from_slice(&0_u64.to_be_bytes());
        storage_logical.extend_from_slice(&slot.0);
        let old_storage_key = full_key(NS_STORAGE, &storage_logical);
        let route = route_key(&old_storage_key).unwrap();
        let partition_ref = manifest.partitions[route.partition as usize];
        if !partition_ref.is_empty() {
            let bytes = get_verified(
                store.as_ref(),
                partition_ref,
                MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES,
            )
            .await
            .unwrap();
            let subshards = decode_checkpoint_partition_manifest(partition_ref, &bytes).unwrap();
            let shard = subshards[checkpoint_subshard(&old_storage_key, subshards.len()).unwrap()];
            if !shard.is_empty() {
                let bytes = get_verified(store.as_ref(), shard, MAX_CHECKPOINT_OBJECT_BYTES)
                    .await
                    .unwrap();
                let entries = decode_checkpoint_partition(shard, &bytes).unwrap();
                assert!(!entries.contains_key(&old_storage_key));
            }
        }
        let far_checkpoint = put_object(
            store.as_ref(),
            &encode_checkpoint_manifest(4_999_000, &legacy_partitions),
        )
        .await
        .unwrap();
        let far_epoch = EpochDescriptor {
            start: 4_999_001,
            end: 5_000_000,
            blocks: ObjectRef {
                digest: Hash32([11; 32]),
                length: 1,
            },
            data: [ObjectRef::default(); DATA_PARTITIONS],
            indexes: [ObjectRef::default(); ROUTER_PARTITIONS],
        };
        let far_directory = Directory {
            window_start: 4_999_001,
            base_checkpoint: far_checkpoint,
            epochs: vec![far_epoch.clone()],
        };
        let far_directory = put_object(store.as_ref(), &encode_directory(&far_directory).unwrap())
            .await
            .unwrap();
        let mut far = publication.clone();
        far.commit.published_number = 5_000_000;
        far.commit.active_window_start = 4_999_001;
        far.commit.active_directory = far_directory;
        let unchanged = far
            .account_at(Address([6; 20]), 5_000_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.balance[31], 2);
        assert!(far
            .account_at(Address([8; 20]), 5_000_000)
            .await
            .unwrap()
            .is_none());

        // A rollover can consume the fixed-eight legacy manifests and emits only
        // the current count-bearing adaptive form.
        let (rolled_checkpoint, _) = build_checkpoint(store.as_ref(), far_checkpoint, &[far_epoch])
            .await
            .unwrap();
        let rolled_bytes =
            get_verified(store.as_ref(), rolled_checkpoint, CHECKPOINT_MANIFEST_BYTES)
                .await
                .unwrap();
        let rolled_manifest = decode_checkpoint_manifest(rolled_checkpoint, &rolled_bytes).unwrap();
        for reference in rolled_manifest
            .partitions
            .iter()
            .copied()
            .filter(|reference| !reference.is_empty())
        {
            let bytes = get_verified(
                store.as_ref(),
                reference,
                MAX_CHECKPOINT_PARTITION_MANIFEST_BYTES,
            )
            .await
            .unwrap();
            assert_eq!(&bytes[..7], b"FSPS\x01\x00\x08");
        }
        let rolled_directory = put_object(
            store.as_ref(),
            &encode_directory(&Directory {
                window_start: 5_000_001,
                base_checkpoint: rolled_checkpoint,
                epochs: vec![EpochDescriptor {
                    start: 5_000_001,
                    end: 5_001_000,
                    blocks: ObjectRef {
                        digest: Hash32([12; 32]),
                        length: 1,
                    },
                    data: [ObjectRef::default(); DATA_PARTITIONS],
                    indexes: [ObjectRef::default(); ROUTER_PARTITIONS],
                }],
            })
            .unwrap(),
        )
        .await
        .unwrap();
        far.commit.published_number = 5_001_000;
        far.commit.active_window_start = 5_000_001;
        far.commit.active_directory = rolled_directory;
        assert_eq!(
            far.account_at(Address([6; 20]), 5_001_000)
                .await
                .unwrap()
                .unwrap()
                .balance[31],
            2
        );
    }

    #[tokio::test]
    async fn startup_is_exactly_head_and_commit_and_encoding_is_deterministic() {
        let first = Arc::new(CountingStore::default());
        let second: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        let outcome = publish(
            first.clone(),
            package(0, true, true),
            PublicationGate::Finalized {
                number: 0,
                hash: hash(0),
            },
        )
        .await
        .unwrap();
        let other = publish(
            second,
            package(0, true, true),
            PublicationGate::Finalized {
                number: 0,
                hash: hash(0),
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.commit, other.commit);
        let before = first.reads.load(Ordering::Relaxed);
        let publication = Publication::load(first.clone(), 1).await.unwrap();
        assert_eq!(first.reads.load(Ordering::Relaxed) - before, 2);
        let mut mismatched = publication;
        mismatched.commit.active_window_start = 1;
        let mut budget = LookupBudget::default();
        assert!(mismatched.directory_at(1, &mut budget).await.is_err());

        let key = head_key(1);
        let current = first.read_mutable(&key).await.unwrap().unwrap();
        let oversized_head = Head {
            generation: 1,
            chain_id: 1,
            number: 0,
            hash: hash(0),
            commit: ObjectRef {
                digest: outcome.commit,
                length: (COMMIT_BYTES + 1) as u32,
            },
        };
        first
            .compare_and_swap(&key, Some(&current), &oversized_head.encode())
            .await
            .unwrap();
        let before = first.reads.load(Ordering::Relaxed);
        let error = Publication::load(first.clone(), 1).await.err().unwrap();
        assert!(error.to_string().contains("fixed commit size"));
        assert_eq!(first.reads.load(Ordering::Relaxed) - before, 1);

        let current = first.read_mutable(&key).await.unwrap().unwrap();
        first
            .compare_and_swap(&key, Some(&current), &[0; HEAD_BYTES + 1])
            .await
            .unwrap();
        let before = first.reads.load(Ordering::Relaxed);
        let error = Publication::load(first.clone(), 1).await.err().unwrap();
        assert!(error.to_string().contains("bounded read limit"));
        assert_eq!(first.reads.load(Ordering::Relaxed) - before, 1);

        let before = first.reads.load(Ordering::Relaxed);
        let oversized = ObjectRef {
            digest: Hash32([5; 32]),
            length: 9,
        };
        assert!(get_verified(first.as_ref(), oversized, 8).await.is_err());
        assert_eq!(first.reads.load(Ordering::Relaxed), before);
    }

    #[tokio::test]
    async fn catalog_range_must_match_decoded_directory() {
        let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
        let valid = ObjectRef {
            digest: Hash32([1; 32]),
            length: 1,
        };
        let directory = Directory {
            window_start: 0,
            base_checkpoint: ObjectRef::default(),
            epochs: vec![EpochDescriptor {
                start: 0,
                end: 0,
                blocks: valid,
                data: [ObjectRef::default(); DATA_PARTITIONS],
                indexes: [ObjectRef::default(); ROUTER_PARTITIONS],
            }],
        };
        let directory_ref = put_object(store.as_ref(), &encode_directory(&directory).unwrap())
            .await
            .unwrap();
        let chunk = CatalogChunk {
            entries: vec![CatalogEntry {
                start: 0,
                end: 1,
                directory: directory_ref,
            }],
        };
        let chunk_ref = put_object(store.as_ref(), &encode_catalog_chunk(&chunk).unwrap())
            .await
            .unwrap();
        let root = CatalogRoot {
            chunks: vec![(0, chunk_ref)],
        };
        let root_ref = put_object(store.as_ref(), &encode_catalog_root(&root).unwrap())
            .await
            .unwrap();
        let commit = Commit {
            generation: 1,
            chain_id: 1,
            genesis_hash: hash(0),
            anchor_number: 0,
            anchor_hash: hash(0),
            published_hash: hash(100),
            published_number: 100,
            published_state_root: Hash32([0; 32]),
            parent: ObjectRef::default(),
            finalized: true,
            input_sha256: Hash32([0; 32]),
            active_window_start: 100,
            active_directory: directory_ref,
            completed_catalog: root_ref,
        };
        let publication = Publication::from_loaded(
            store.clone(),
            Head {
                generation: 1,
                chain_id: 1,
                number: 100,
                hash: hash(100),
                commit: valid,
            },
            commit,
        );
        let mut budget = LookupBudget::default();
        assert!(publication.directory_at(0, &mut budget).await.is_err());

        let commit = Commit {
            generation: 1,
            chain_id: 1,
            genesis_hash: hash(0),
            anchor_number: 0,
            anchor_hash: hash(0),
            published_hash: hash(1),
            published_number: 1,
            published_state_root: Hash32([0; 32]),
            parent: ObjectRef::default(),
            finalized: true,
            input_sha256: Hash32([0; 32]),
            active_window_start: 0,
            active_directory: directory_ref,
            completed_catalog: ObjectRef::default(),
        };
        let publication = Publication::from_loaded(
            store,
            Head {
                generation: 1,
                chain_id: 1,
                number: 1,
                hash: hash(1),
                commit: valid,
            },
            commit,
        );
        let mut budget = LookupBudget::default();
        let error = publication.directory_at(1, &mut budget).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("missing the committed latest epoch"));
    }

    #[test]
    fn digest_valid_malformed_objects_fail_closed_and_budgets_are_bounded() {
        let mut body = Vec::new();
        put_uleb(&mut body, 1);
        put_bytes(&mut body, &[NS_ACCOUNT, 1]);
        put_uleb(&mut body, 0);
        let bytes = encode_compressed(DATA_MAGIC, &body, MAX_DATA_DECODED).unwrap();
        let reference = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        assert!(decode_data(reference, &bytes)
            .unwrap_err()
            .to_string()
            .contains("zero versions"));

        let valid = ObjectRef {
            digest: Hash32([1; 32]),
            length: 1,
        };
        let epoch = |start, end| EpochDescriptor {
            start,
            end,
            blocks: valid,
            data: [ObjectRef::default(); DATA_PARTITIONS],
            indexes: [ObjectRef::default(); ROUTER_PARTITIONS],
        };
        let directory = Directory {
            window_start: 0,
            base_checkpoint: ObjectRef::default(),
            epochs: vec![epoch(0, u64::MAX), epoch(0, 0)],
        };
        let bytes = encode_directory(&directory).unwrap();
        let reference = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        assert!(decode_directory(reference, &bytes).is_err());

        let compressed = zstd::encode_all([0_u8].as_slice(), 9).unwrap();
        let mut oversized = Vec::new();
        oversized.extend_from_slice(CHECKPOINT_MAGIC);
        oversized.push(VERSION);
        put_u32(&mut oversized, (MAX_CHECKPOINT_SHARD_DECODED + 1) as u32);
        put_u32(&mut oversized, compressed.len() as u32);
        oversized.extend_from_slice(&compressed);
        let reference = ObjectRef {
            digest: Hash32::digest(&oversized),
            length: oversized.len() as u32,
        };
        assert!(decode_checkpoint_partition(reference, &oversized).is_err());

        let mut runs = BTreeMap::new();
        let checkpoint_key = full_key(NS_ACCOUNT, &[3; 20]);
        runs.insert(
            checkpoint_key.clone(),
            vec![
                Version {
                    block: 0,
                    value: vec![0],
                },
                Version {
                    block: 1,
                    value: vec![1],
                },
            ],
        );
        let data_bytes = encode_data(&runs).unwrap();
        let data_ref = ObjectRef {
            digest: Hash32::digest(&data_bytes),
            length: data_bytes.len() as u32,
        };
        let decoded = decode_data(data_ref, &data_bytes).unwrap();
        let mut pointers = BTreeMap::new();
        pointers.insert(
            checkpoint_key,
            StatePointer {
                object: data_ref,
                run_index: 0,
                version_index: 0,
            },
        );
        let pointer_bytes = encode_checkpoint_partition(&pointers).unwrap();
        let pointer_ref = ObjectRef {
            digest: Hash32::digest(&pointer_bytes),
            length: pointer_bytes.len() as u32,
        };
        let decoded_pointers = decode_checkpoint_partition(pointer_ref, &pointer_bytes).unwrap();
        let pointer = *decoded_pointers.values().next().unwrap();
        assert!(checkpoint_version(&decoded.runs[0].1, pointer.version_index, 1).is_err());
        assert!(checkpoint_version(&decoded.runs[0].1, 1, 0).is_err());

        let oversized_code = encode_code_meta(
            0,
            ObjectRef {
                digest: Hash32([4; 32]),
                length: (MAX_CODE_OBJECT_BYTES + 1) as u32,
            },
        );
        assert!(decode_code_meta(&oversized_code).is_err());

        let mut budget = LookupBudget::default();
        for _ in 0..MAX_LOOKUP_REMOTE_GETS {
            budget.remote_get().unwrap();
        }
        assert!(budget.remote_get().is_err());
        let mut budget = LookupBudget::default();
        assert!(budget
            .decoded((MAX_LOOKUP_DECODED_BYTES + 1) as usize)
            .is_err());
    }

    #[test]
    fn corrupt_epoch_objects_fail_closed() {
        let mut runs = BTreeMap::new();
        runs.insert(
            vec![1],
            vec![Version {
                block: 1,
                value: vec![2],
            }],
        );
        let bytes = encode_data(&runs).unwrap();
        let reference = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(decode_data(reference, &corrupt).is_err());

        let index = IndexDelta {
            partition: 0,
            epoch_start: 0,
            dictionary: vec![reference],
            entries: Vec::new(),
        };
        let bytes = encode_index(&index).unwrap();
        let index_ref = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(decode_index(index_ref, &corrupt).is_err());

        let directory = Directory {
            window_start: 0,
            base_checkpoint: ObjectRef::default(),
            epochs: Vec::new(),
        };
        let bytes = encode_directory(&directory).unwrap();
        let reference = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(decode_directory(reference, &corrupt).is_err());

        let manifest_bytes =
            encode_checkpoint_manifest(0, &[ObjectRef::default(); ROUTER_PARTITIONS]);
        let manifest_ref = ObjectRef {
            digest: Hash32::digest(&manifest_bytes),
            length: manifest_bytes.len() as u32,
        };
        let mut corrupt_manifest = manifest_bytes;
        corrupt_manifest[0] ^= 1;
        assert!(decode_checkpoint_manifest(manifest_ref, &corrupt_manifest).is_err());

        let entries = BTreeMap::new();
        let bytes = encode_checkpoint_partition(&entries).unwrap();
        let reference = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(decode_checkpoint_partition(reference, &corrupt).is_err());

        let catalog = CatalogChunk {
            entries: vec![CatalogEntry {
                start: 0,
                end: 1,
                directory: reference,
            }],
        };
        let bytes = encode_catalog_chunk(&catalog).unwrap();
        let catalog_ref = ObjectRef {
            digest: Hash32::digest(&bytes),
            length: bytes.len() as u32,
        };
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(decode_catalog_chunk(catalog_ref, &corrupt).is_err());

        let mut commit = vec![0; COMMIT_BYTES];
        commit[..4].copy_from_slice(COMMIT_MAGIC);
        assert!(Commit::decode(&commit).is_err());
    }
}
