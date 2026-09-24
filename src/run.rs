//! Immutable, hash-routed sorted-run codec for time-tiered historical state.
//!
//! The v1 format has 16 hash partitions, data pages capped at 1 MiB encoded and
//! decoded, and exact fence leaves and internal roots capped at 512 KiB each.
//! Lookups read the run root, optionally one internal root, one fence leaf, and
//! at most one data page. Builder memory is bounded by one fence leaf and one
//! parent index per partition.

use crate::format::Hash32;
use anyhow::{bail, Context, Result};
use std::collections::VecDeque;
use std::io::{Read, Write};

const ROOT_MAGIC: &[u8; 4] = b"FTRT";
const INDEX_MAGIC: &[u8; 4] = b"FTIX";
const DATA_MAGIC: &[u8; 4] = b"FTDP";
const INTERNAL_MAGIC: &[u8; 4] = b"FTIR";
const COMPRESSED_MAGIC: &[u8; 4] = b"FTRZ";
const VERSION: u8 = 1;
const PARTITIONS: usize = 16;
const MAX_PAGE_BYTES: usize = 1024 * 1024;
const MAX_INDEX_BYTES: usize = 512 * 1024;
const ROOT_BYTES: usize = 4 + 1 + PARTITIONS * 36;
const REF_BYTES: usize = 36;

/// An archive-compatible reference: SHA-256 of exact encoded bytes and exact length.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectRef {
    pub digest: Hash32,
    pub length: u32,
}

/// Immutable object ready for `ArchiveStore::put_immutable`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedObject {
    pub reference: ObjectRef,
    pub bytes: Vec<u8>,
}

impl EncodedObject {
    /// The same content-addressed key convention used by `ArchiveStore`.
    pub fn object_key(&self) -> String {
        self.reference.digest.object_key()
    }
}

/// One exact state version. Empty values are retained as explicit tombstones/zeros.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub key: Vec<u8>,
    pub block: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Fence {
    key: Vec<u8>,
    block: u64,
    data: ObjectRef,
}

/// Stateful bounded streaming builder. Input must be strictly sorted by `(key, block)`.
pub struct RunBuilder;

impl RunBuilder {
    /// Encode sorted records, emitting immutable data/fence/root objects in dependency order.
    /// Only data-page buffers, current fence leaves, and capped parent indexes are retained.
    pub fn build<I, F>(records: I, mut emit: F) -> Result<ObjectRef>
    where
        I: IntoIterator<Item = Record>,
        F: FnMut(EncodedObject) -> Result<()>,
    {
        let mut pages: [Vec<u8>; PARTITIONS] = std::array::from_fn(|_| data_page_start());
        let mut counts = [0_u32; PARTITIONS];
        let mut leaves: [Vec<Fence>; PARTITIONS] = std::array::from_fn(|_| Vec::new());
        let mut parents: [Vec<Fence>; PARTITIONS] = std::array::from_fn(|_| Vec::new());
        let mut previous: Option<(Vec<u8>, u64)> = None;

        for record in records {
            if record.key.is_empty() {
                bail!("run record key must not be empty");
            }
            let ordering = (&record.key, record.block);
            if previous
                .as_ref()
                .is_some_and(|(key, block)| (key, *block) >= ordering)
            {
                bail!("run records must be strictly sorted by (key, block)");
            }
            let record_size = record
                .key
                .len()
                .checked_add(record.value.len())
                .and_then(|size| size.checked_add(16 + 9))
                .context("run record length overflow")?;
            if record_size > MAX_PAGE_BYTES {
                bail!("single run record exceeds 1 MiB data-page bound");
            }
            let partition = partition(&record.key);
            let encoded_record = encode_record(&record)?;
            if pages[partition].len() + encoded_record.len() > MAX_PAGE_BYTES {
                if counts[partition] == 0 {
                    bail!("single run record exceeds 1 MiB data-page bound");
                }
                flush_page(
                    partition,
                    &mut pages,
                    &mut counts,
                    &mut leaves,
                    &mut parents,
                    &mut emit,
                )?;
                if pages[partition].len() + encoded_record.len() > MAX_PAGE_BYTES {
                    bail!("single run record exceeds 1 MiB data-page bound");
                }
            }
            pages[partition].extend_from_slice(&encoded_record);
            counts[partition] += 1;
            previous = Some((record.key.clone(), record.block));
        }

        for partition in 0..PARTITIONS {
            if counts[partition] != 0 {
                flush_page(
                    partition,
                    &mut pages,
                    &mut counts,
                    &mut leaves,
                    &mut parents,
                    &mut emit,
                )?;
            }
            if !leaves[partition].is_empty() {
                emit_leaf(partition, &mut leaves, &mut parents, &mut emit)?;
            }
        }

        let mut root = Vec::with_capacity(ROOT_BYTES);
        root.extend_from_slice(ROOT_MAGIC);
        root.push(VERSION);
        for entries in &parents {
            let reference = match entries.as_slice() {
                [] => ObjectRef::default(),
                [only] => only.data,
                _ => {
                    let bytes = encode_index(entries, INTERNAL_MAGIC)?;
                    let object = make_object(bytes)?;
                    let reference = object.reference;
                    emit(object).context("emit run internal fence root")?;
                    reference
                }
            };
            put_ref(&mut root, reference);
        }
        let root = make_object(root)?;
        let reference = root.reference;
        emit(root).context("emit run root")?;
        Ok(reference)
    }
}

/// Decoded run directory. The caller supplies its already fetched root bytes.
#[derive(Clone, Debug)]
pub struct RunReader {
    indexes: [ObjectRef; PARTITIONS],
}

impl RunReader {
    pub fn decode(reference: ObjectRef, bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ROOT_BYTES {
            bail!("invalid run root length");
        }
        verify_object(reference, bytes)?;
        if bytes.len() != ROOT_BYTES || &bytes[..4] != ROOT_MAGIC || bytes[4] != VERSION {
            bail!("invalid run root");
        }
        let mut indexes = [ObjectRef::default(); PARTITIONS];
        for (index, slot) in indexes.iter_mut().enumerate() {
            *slot = read_ref(bytes, 5 + index * REF_BYTES)?;
            if *slot != ObjectRef::default()
                && (slot.length == 0
                    || slot.digest == Hash32([0; 32])
                    || slot.length as usize > MAX_INDEX_BYTES)
            {
                bail!("invalid run fence reference in root");
            }
        }
        Ok(Self { indexes })
    }

    /// Create a bounded scanner over all routed partitions of this run.
    pub fn scanner(&self) -> RunScanner {
        RunScanner {
            cursors: self.indexes.map(PartitionCursor::new),
            failed: false,
        }
    }

    /// Return the newest value for `key` at or before `block`. Fetches at most
    /// the optional internal root, exact fence leaf, and one data page.
    pub async fn lookup<F, Fut>(
        &self,
        key: &[u8],
        block: u64,
        mut fetch: F,
    ) -> Result<Option<Vec<u8>>>
    where
        F: FnMut(ObjectRef) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        if key.is_empty() {
            return Ok(None);
        }
        let shard = partition(key);
        let root_ref = self.indexes[shard];
        if root_ref == ObjectRef::default() {
            return Ok(None);
        }
        validate_index_ref(root_ref)?;
        let root_bytes = fetch(root_ref).await.context("fetch run fence root")?;
        let (leaf_ref, leaf_bytes, expected_start) = if root_bytes.starts_with(INTERNAL_MAGIC) {
            let parents = decode_index(root_ref, &root_bytes, shard, INTERNAL_MAGIC)?;
            let target = (key, block);
            let position = parents.partition_point(|entry| (&entry.key[..], entry.block) <= target);
            if position == 0 {
                return Ok(None);
            }
            let entry = &parents[position - 1];
            validate_index_ref(entry.data)?;
            let bytes = fetch(entry.data).await.context("fetch run fence leaf")?;
            (entry.data, bytes, Some((entry.key.clone(), entry.block)))
        } else {
            (root_ref, root_bytes, None)
        };
        let fences = decode_index(leaf_ref, &leaf_bytes, shard, INDEX_MAGIC)?;
        if expected_start.is_some_and(|start| {
            fences
                .first()
                .is_none_or(|fence| (&fence.key[..], fence.block) != (&start.0[..], start.1))
        }) {
            bail!("run fence leaf does not match its parent entry");
        }
        let target = (key, block);
        let position = fences.partition_point(|fence| (&fence.key[..], fence.block) <= target);
        if position == 0 {
            return Ok(None);
        }
        let fence = &fences[position - 1];
        let data_ref = fence.data;
        if data_ref.length == 0
            || data_ref.length as usize > MAX_PAGE_BYTES
            || data_ref.digest == Hash32([0; 32])
        {
            bail!("run data reference exceeds the 1 MiB pre-fetch bound");
        }
        let page_bytes = fetch(data_ref).await.context("fetch run data page")?;
        let records = decode_data_page(data_ref, &page_bytes, shard)?;
        if !records
            .first()
            .is_some_and(|first| (&first.key[..], first.block) == (&fence.key[..], fence.block))
        {
            bail!("run data page does not match its exact fence");
        }
        let upper = records.partition_point(|record| (&record.key[..], record.block) <= target);
        if upper == 0 {
            return Ok(None);
        }
        let candidate = &records[upper - 1];
        if candidate.key == key {
            Ok(Some(candidate.value.clone()))
        } else {
            Ok(None)
        }
    }
}

/// Streams exact run records in global `(key, block)` order. Holds at most one
/// decoded data page and fence leaf per partition, plus a bounded parent index.
pub struct RunScanner {
    cursors: [PartitionCursor; PARTITIONS],
    failed: bool,
}

struct PartitionCursor {
    root: ObjectRef,
    initialized: bool,
    parents: Vec<Fence>,
    parent_pos: usize,
    leaf: Vec<Fence>,
    fence_pos: usize,
    page: VecDeque<Record>,
    previous: Option<(Vec<u8>, u64)>,
    previous_fence: Option<(Vec<u8>, u64)>,
}

impl PartitionCursor {
    fn new(root: ObjectRef) -> Self {
        Self {
            root,
            initialized: false,
            parents: Vec::new(),
            parent_pos: 0,
            leaf: Vec::new(),
            fence_pos: 0,
            page: VecDeque::new(),
            previous: None,
            previous_fence: None,
        }
    }

    async fn ready<F, Fut>(&mut self, shard: usize, fetch: &mut F) -> Result<()>
    where
        F: FnMut(ObjectRef) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        if !self.initialized {
            self.initialized = true;
            if self.root == ObjectRef::default() {
                return Ok(());
            }
            validate_index_ref(self.root)?;
            let bytes = fetch(self.root).await.context("fetch run fence root")?;
            if bytes.starts_with(INTERNAL_MAGIC) {
                self.parents = decode_index(self.root, &bytes, shard, INTERNAL_MAGIC)?;
            } else {
                self.leaf = decode_index(self.root, &bytes, shard, INDEX_MAGIC)?;
            }
        }
        loop {
            if !self.page.is_empty() {
                return Ok(());
            }
            if self.fence_pos == self.leaf.len() {
                // Drop the previous leaf before loading another one.
                self.leaf.clear();
                self.fence_pos = 0;
                let Some(parent) = self.parents.get(self.parent_pos) else {
                    return Ok(());
                };
                validate_index_ref(parent.data)?;
                let bytes = fetch(parent.data).await.context("fetch run fence leaf")?;
                let leaf = decode_index(parent.data, &bytes, shard, INDEX_MAGIC)?;
                if leaf.first().is_none_or(|first| {
                    (first.key.as_slice(), first.block) != (parent.key.as_slice(), parent.block)
                }) {
                    bail!("run fence leaf does not match its parent entry");
                }
                self.leaf = leaf;
                self.parent_pos += 1;
            }
            let fence = &self.leaf[self.fence_pos];
            let start = (fence.key.as_slice(), fence.block);
            if self
                .previous_fence
                .as_ref()
                .is_some_and(|(key, block)| (key.as_slice(), *block) >= start)
            {
                bail!("run fence pages are not strictly ordered");
            }
            self.previous_fence = Some((fence.key.clone(), fence.block));
            self.fence_pos += 1;
            if fence.data.length == 0
                || fence.data.length as usize > MAX_PAGE_BYTES
                || fence.data.digest == Hash32([0; 32])
            {
                bail!("run data reference exceeds the 1 MiB pre-fetch bound");
            }
            let bytes = fetch(fence.data).await.context("fetch run data page")?;
            let records = decode_data_page(fence.data, &bytes, shard)?;
            let first = &records[0];
            if (first.key.as_slice(), first.block) != start {
                bail!("run data page does not match its exact fence");
            }
            if self
                .previous
                .as_ref()
                .is_some_and(|(key, block)| (key.as_slice(), *block) >= start)
            {
                bail!("run data pages are not strictly ordered");
            }
            self.page = records.into();
        }
    }
}

impl RunScanner {
    /// Return the next exact record, or `None` after the run is exhausted.
    /// `fetch` must retrieve encoded objects by their content-addressed reference.
    pub async fn next<F, Fut>(&mut self, mut fetch: F) -> Result<Option<Record>>
    where
        F: FnMut(ObjectRef) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        if self.failed {
            bail!("run scanner is invalid after a failed read");
        }
        for (shard, cursor) in self.cursors.iter_mut().enumerate() {
            if let Err(error) = cursor.ready(shard, &mut fetch).await {
                self.failed = true;
                return Err(error);
            }
        }
        let chosen = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(shard, cursor)| cursor.page.front().map(|record| (shard, record)))
            .min_by(|(_, left), (_, right)| (&left.key, left.block).cmp(&(&right.key, right.block)))
            .map(|(shard, _)| shard);
        let Some(shard) = chosen else {
            return Ok(None);
        };
        let cursor = &mut self.cursors[shard];
        let record = cursor.page.pop_front().expect("chosen cursor has a record");
        cursor.previous = Some((record.key.clone(), record.block));
        Ok(Some(record))
    }
}

fn validate_index_ref(reference: ObjectRef) -> Result<()> {
    if reference.length == 0
        || reference.length as usize > MAX_INDEX_BYTES
        || reference.digest == Hash32([0; 32])
    {
        bail!("run fence reference exceeds the 512 KiB pre-fetch bound");
    }
    Ok(())
}

fn partition(key: &[u8]) -> usize {
    (Hash32::digest(key).0[0] >> 4) as usize
}

fn data_page_start() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4096);
    bytes.extend_from_slice(DATA_MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes
}

fn encode_record(record: &Record) -> Result<Vec<u8>> {
    let key_len = u32::try_from(record.key.len()).context("run key too long")?;
    let value_len = u32::try_from(record.value.len()).context("run value too long")?;
    let mut bytes = Vec::with_capacity(16 + record.key.len() + record.value.len());
    bytes.extend_from_slice(&key_len.to_be_bytes());
    bytes.extend_from_slice(&record.key);
    bytes.extend_from_slice(&record.block.to_be_bytes());
    bytes.extend_from_slice(&value_len.to_be_bytes());
    bytes.extend_from_slice(&record.value);
    Ok(bytes)
}

fn flush_page<F>(
    partition: usize,
    pages: &mut [Vec<u8>; PARTITIONS],
    counts: &mut [u32; PARTITIONS],
    leaves: &mut [Vec<Fence>; PARTITIONS],
    parents: &mut [Vec<Fence>; PARTITIONS],
    emit: &mut F,
) -> Result<()>
where
    F: FnMut(EncodedObject) -> Result<()>,
{
    let count = counts[partition];
    if count == 0 {
        return Ok(());
    }
    pages[partition][5..9].copy_from_slice(&count.to_be_bytes());
    let first = decode_first_record(&pages[partition])?;
    let mut encoder = zstd::Encoder::new(Vec::new(), 3)?;
    encoder.window_log(20)?;
    encoder.write_all(&pages[partition])?;
    let compressed = encoder.finish()?;
    let mut bytes = Vec::with_capacity(13 + compressed.len());
    bytes.extend_from_slice(COMPRESSED_MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&(pages[partition].len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&compressed);
    if pages[partition].len() > MAX_PAGE_BYTES || bytes.len() > MAX_PAGE_BYTES {
        bail!("run data page exceeds 1 MiB encoded or decoded bound");
    }
    let object = make_object(bytes)?;
    let reference = object.reference;
    emit(object).context("emit run data page")?;
    append_leaf_fence(
        partition,
        Fence {
            key: first.key,
            block: first.block,
            data: reference,
        },
        leaves,
        parents,
        emit,
    )?;
    pages[partition] = data_page_start();
    counts[partition] = 0;
    Ok(())
}

fn append_leaf_fence<F>(
    partition: usize,
    fence: Fence,
    leaves: &mut [Vec<Fence>; PARTITIONS],
    parents: &mut [Vec<Fence>; PARTITIONS],
    emit: &mut F,
) -> Result<()>
where
    F: FnMut(EncodedObject) -> Result<()>,
{
    leaves[partition].push(fence);
    if index_size(&leaves[partition])? <= MAX_INDEX_BYTES {
        return Ok(());
    }
    let last = leaves[partition].pop().expect("just pushed fence");
    if leaves[partition].is_empty() {
        bail!("single run fence exceeds 512 KiB leaf bound");
    }
    emit_leaf(partition, leaves, parents, emit)?;
    leaves[partition].push(last);
    if index_size(&leaves[partition])? > MAX_INDEX_BYTES {
        bail!("single run fence exceeds 512 KiB leaf bound");
    }
    Ok(())
}

fn emit_leaf<F>(
    partition: usize,
    leaves: &mut [Vec<Fence>; PARTITIONS],
    parents: &mut [Vec<Fence>; PARTITIONS],
    emit: &mut F,
) -> Result<()>
where
    F: FnMut(EncodedObject) -> Result<()>,
{
    let entries = &leaves[partition];
    if entries.is_empty() {
        return Ok(());
    }
    let first = &entries[0];
    let bytes = encode_index(entries, INDEX_MAGIC)?;
    let object = make_object(bytes)?;
    let reference = object.reference;
    let parent_entry = Fence {
        key: first.key.clone(),
        block: first.block,
        data: reference,
    };
    if parents[partition].last().is_some_and(|prior| {
        (&prior.key[..], prior.block) >= (&parent_entry.key[..], parent_entry.block)
    }) {
        bail!("run fence leaves are not strictly ordered");
    }
    parents[partition].push(parent_entry);
    if index_size(&parents[partition])? > MAX_INDEX_BYTES {
        parents[partition].pop();
        bail!("run internal fence root exceeds 512 KiB bound");
    }
    emit(object).context("emit run fence leaf")?;
    leaves[partition].clear();
    Ok(())
}

fn decode_first_record(page: &[u8]) -> Result<Record> {
    let records = parse_data_payload(page, None)?;
    records
        .into_iter()
        .next()
        .context("run data page unexpectedly empty")
}

fn parse_data_payload(bytes: &[u8], expected_partition: Option<usize>) -> Result<Vec<Record>> {
    if bytes.len() < 9 || &bytes[..4] != DATA_MAGIC || bytes[4] != VERSION {
        bail!("invalid run data page header");
    }
    let count = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let mut cursor = 9;
    let mut records = Vec::new();
    if count > bytes.len().saturating_sub(9) / 17 {
        bail!("run data page record count exceeds encoded payload bound");
    }
    records.try_reserve_exact(count)?;
    for _ in 0..count {
        let key_len = read_u32(bytes, &mut cursor)? as usize;
        let key_end = cursor
            .checked_add(key_len)
            .context("run key length overflow")?;
        let key = bytes
            .get(cursor..key_end)
            .context("truncated run key")?
            .to_vec();
        cursor = key_end;
        let block = read_u64(bytes, &mut cursor)?;
        let value_len = read_u32(bytes, &mut cursor)? as usize;
        let value_end = cursor
            .checked_add(value_len)
            .context("run value length overflow")?;
        let value = bytes
            .get(cursor..value_end)
            .context("truncated run value")?
            .to_vec();
        cursor = value_end;
        if key.is_empty() || expected_partition.is_some_and(|p| partition(&key) != p) {
            bail!("run page contains invalid or misrouted full key");
        }
        let record = Record { key, block, value };
        if records.last().is_some_and(|prior: &Record| {
            (&prior.key[..], prior.block) >= (&record.key[..], record.block)
        }) {
            bail!("run data page records are not strictly sorted");
        }
        records.push(record);
    }
    if cursor != bytes.len() || records.is_empty() {
        bail!("run data page length or record count mismatch");
    }
    Ok(records)
}

fn decode_data_page(reference: ObjectRef, bytes: &[u8], shard: usize) -> Result<Vec<Record>> {
    let decoded = decode_compressed(reference, bytes, MAX_PAGE_BYTES)?;
    parse_data_payload(&decoded, Some(shard))
}

fn index_size(entries: &[Fence]) -> Result<usize> {
    let mut size = 9_usize;
    for fence in entries {
        size = size
            .checked_add(4 + fence.key.len() + 8 + REF_BYTES)
            .context("run index size overflow")?;
    }
    Ok(size)
}

fn encode_index(entries: &[Fence], magic: &[u8; 4]) -> Result<Vec<u8>> {
    let size = index_size(entries)?;
    if size > MAX_INDEX_BYTES {
        bail!("run fence index exceeds 512 KiB bound");
    }
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(magic);
    bytes.push(VERSION);
    bytes.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for fence in entries {
        put_bytes(&mut bytes, &fence.key)?;
        bytes.extend_from_slice(&fence.block.to_be_bytes());
        put_ref(&mut bytes, fence.data);
    }
    Ok(bytes)
}

fn decode_index(
    reference: ObjectRef,
    bytes: &[u8],
    shard: usize,
    magic: &[u8; 4],
) -> Result<Vec<Fence>> {
    if bytes.len() > MAX_INDEX_BYTES {
        bail!("run fence index exceeds 512 KiB pre-decode bound");
    }
    verify_object(reference, bytes)?;
    if bytes.len() > MAX_INDEX_BYTES
        || bytes.len() < 9
        || &bytes[..4] != magic
        || bytes[4] != VERSION
    {
        bail!("invalid or oversized run fence index");
    }
    let count = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let mut cursor = 9;
    let mut fences = Vec::new();
    if count > bytes.len().saturating_sub(9) / (4 + 1 + 8 + REF_BYTES) {
        bail!("run fence count exceeds encoded index bound");
    }
    fences.try_reserve_exact(count)?;
    for _ in 0..count {
        let key_len = read_u32(bytes, &mut cursor)? as usize;
        let end = cursor
            .checked_add(key_len)
            .context("run fence key overflow")?;
        let key = bytes
            .get(cursor..end)
            .context("truncated run fence key")?
            .to_vec();
        cursor = end;
        let block = read_u64(bytes, &mut cursor)?;
        let data = read_ref(bytes, cursor)?;
        cursor += REF_BYTES;
        if key.is_empty()
            || partition(&key) != shard
            || data == ObjectRef::default()
            || data.length == 0
            || data.length as usize
                > if magic == INTERNAL_MAGIC {
                    MAX_INDEX_BYTES
                } else {
                    MAX_PAGE_BYTES
                }
            || data.digest == Hash32([0; 32])
        {
            bail!("invalid run fence");
        }
        let fence = Fence { key, block, data };
        if fences.last().is_some_and(|prior: &Fence| {
            (&prior.key[..], prior.block) >= (&fence.key[..], fence.block)
        }) {
            bail!("run fences are not strictly sorted");
        }
        fences.push(fence);
    }
    if cursor != bytes.len() || fences.is_empty() {
        bail!("run fence index length or count mismatch");
    }
    Ok(fences)
}

fn decode_compressed(reference: ObjectRef, bytes: &[u8], maximum: usize) -> Result<Vec<u8>> {
    if bytes.len() > maximum {
        bail!("compressed run page exceeds encoded bound");
    }
    verify_object(reference, bytes)?;
    if bytes.len() > maximum
        || bytes.len() < 13
        || &bytes[..4] != COMPRESSED_MAGIC
        || bytes[4] != VERSION
    {
        bail!("invalid or oversized compressed run page");
    }
    let decoded_len = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let compressed_len = u32::from_be_bytes(bytes[9..13].try_into().unwrap()) as usize;
    if decoded_len > maximum || compressed_len != bytes.len() - 13 {
        bail!("compressed run page length exceeds bound or mismatches header");
    }
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(decoded_len)?;
    let mut decoder = zstd::Decoder::new(&bytes[13..])?;
    decoder.window_log_max(20)?;
    decoder
        .take((maximum + 1) as u64)
        .read_to_end(&mut decoded)?;
    if decoded.len() != decoded_len {
        bail!("compressed run page decoded length mismatch");
    }
    Ok(decoded)
}

fn make_object(bytes: Vec<u8>) -> Result<EncodedObject> {
    let length = u32::try_from(bytes.len()).context("run object exceeds u32 reference length")?;
    Ok(EncodedObject {
        reference: ObjectRef {
            digest: Hash32::digest(&bytes),
            length,
        },
        bytes,
    })
}

fn verify_object(reference: ObjectRef, bytes: &[u8]) -> Result<()> {
    if reference.length as usize != bytes.len() || reference.digest != Hash32::digest(bytes) {
        bail!("run object reference length or SHA-256 mismatch");
    }
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let length = u32::try_from(bytes.len()).context("run key exceeds u32 length")?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_ref(out: &mut Vec<u8>, reference: ObjectRef) {
    out.extend_from_slice(&reference.digest.0);
    out.extend_from_slice(&reference.length.to_be_bytes());
}

fn read_ref(bytes: &[u8], offset: usize) -> Result<ObjectRef> {
    let end = offset
        .checked_add(REF_BYTES)
        .context("run ref offset overflow")?;
    let raw = bytes.get(offset..end).context("truncated run object ref")?;
    Ok(ObjectRef {
        digest: Hash32(raw[..32].try_into().unwrap()),
        length: u32::from_be_bytes(raw[32..36].try_into().unwrap()),
    })
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor.checked_add(4).context("run cursor overflow")?;
    let value = u32::from_be_bytes(
        bytes
            .get(*cursor..end)
            .context("truncated run u32")?
            .try_into()
            .unwrap(),
    );
    *cursor = end;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor.checked_add(8).context("run cursor overflow")?;
    let value = u64::from_be_bytes(
        bytes
            .get(*cursor..end)
            .context("truncated run u64")?
            .try_into()
            .unwrap(),
    );
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn records(key: &[u8], versions: &[(u64, &[u8])]) -> Vec<Record> {
        versions
            .iter()
            .map(|(block, value)| Record {
                key: key.to_vec(),
                block: *block,
                value: value.to_vec(),
            })
            .collect()
    }

    fn build(records: Vec<Record>) -> (RunReader, HashMap<Hash32, Vec<u8>>, usize) {
        let mut objects = HashMap::new();
        let mut root = None;
        let root_ref = RunBuilder::build(records, |object| {
            if object.bytes.starts_with(ROOT_MAGIC) {
                root = Some((object.reference, object.bytes.clone()));
            }
            objects.insert(object.reference.digest, object.bytes);
            Ok(())
        })
        .unwrap();
        let (ref_from_emit, bytes) = root.unwrap();
        assert_eq!(root_ref, ref_from_emit);
        let reader = RunReader::decode(root_ref, &bytes).unwrap();
        (reader, objects, bytes.len())
    }

    #[tokio::test]
    async fn exact_predecessor_preserves_zero_and_tombstone() {
        let mut input = records(b"account-key", &[(0, b"initial"), (4, b""), (9, b"\0")]);
        let (reader, objects, _) = build(std::mem::take(&mut input));
        let mut gets = 0;
        assert_eq!(
            reader
                .lookup(b"account-key", 3, |reference| {
                    gets += 1;
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            Some(b"initial".to_vec())
        );
        assert_eq!(gets, 2);
        assert_eq!(
            reader
                .lookup(b"account-key", 4, |reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            Some(Vec::new())
        );
        assert_eq!(
            reader
                .lookup(b"account-key", 9, |reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            Some(vec![0])
        );
        assert_eq!(
            reader
                .lookup(b"account-key", 99, |reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            Some(vec![0])
        );
        assert_eq!(
            reader
                .lookup(b"absent", 99, |reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn hot_key_splits_across_pages_and_uses_block_upper_bound() {
        let key = b"hot-key";
        let value = vec![7; 24 * 1024];
        let input = (0..80).map(|block| Record {
            key: key.to_vec(),
            block,
            value: value.clone(),
        });
        let (reader, objects, _) = build(input.collect());
        let mut calls = 0;
        let found = reader
            .lookup(key, 47, |reference| {
                calls += 1;
                let bytes = objects[&reference.digest].clone();
                async move { Ok(bytes) }
            })
            .await
            .unwrap();
        assert_eq!(found, Some(value));
        assert!(calls <= 2);
    }

    #[tokio::test]
    async fn second_fence_leaf_routes_hot_key_by_block() {
        let key = vec![b'h'; 150 * 1024];
        let input = (0..4).map(|block| Record {
            key: key.clone(),
            block,
            value: vec![block as u8; 400 * 1024],
        });
        let (reader, objects, _) = build(input.collect());
        let shard = partition(&key);
        let parent_ref = reader.indexes[shard];
        let parent = &objects[&parent_ref.digest];
        assert!(parent.starts_with(INTERNAL_MAGIC));
        let entries = decode_index(parent_ref, parent, shard, INTERNAL_MAGIC).unwrap();
        assert_eq!(entries.len(), 2);
        for block in [0, 2, 3, 4] {
            let mut calls = 0;
            assert_eq!(
                reader
                    .lookup(&key, block, |reference| {
                        calls += 1;
                        let bytes = objects[&reference.digest].clone();
                        async move { Ok(bytes) }
                    })
                    .await
                    .unwrap(),
                Some(vec![block.min(3) as u8; 400 * 1024])
            );
            assert_eq!(calls, 3);
        }
    }

    #[tokio::test]
    async fn lookup_accepts_yielding_fetch() {
        let (reader, objects, _) = build(records(b"async-key", &[(7, b"value")]));
        let mut calls = 0;
        let found = reader
            .lookup(b"async-key", 7, |reference| {
                calls += 1;
                let bytes = objects[&reference.digest].clone();
                async move {
                    tokio::task::yield_now().await;
                    Ok(bytes)
                }
            })
            .await
            .unwrap();
        assert_eq!(found, Some(b"value".to_vec()));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn scanner_interleaves_partitions_and_preserves_empty_values() {
        let keys: Vec<Vec<u8>> = (0..32)
            .map(|i| format!("key-{i:02}").into_bytes())
            .collect();
        assert!(keys
            .windows(2)
            .any(|pair| partition(&pair[0]) != partition(&pair[1])));
        let mut input: Vec<Record> = keys
            .iter()
            .flat_map(|key| records(key, &[(0, b""), (1, b"\0")]))
            .collect();
        input.sort_by(|a, b| (&a.key, a.block).cmp(&(&b.key, b.block)));
        let (reader, objects, _) = build(input.clone());
        let mut scanner = reader.scanner();
        let mut output = Vec::new();
        while let Some(record) = scanner
            .next(|reference| {
                let bytes = objects[&reference.digest].clone();
                async move {
                    tokio::task::yield_now().await;
                    Ok(bytes)
                }
            })
            .await
            .unwrap()
        {
            output.push(record);
            assert!(scanner.cursors.iter().all(|cursor| {
                cursor
                    .page
                    .iter()
                    .map(|record| 25 + record.key.len() + record.value.len())
                    .sum::<usize>()
                    <= MAX_PAGE_BYTES
                    && index_size(&cursor.leaf).unwrap() <= MAX_INDEX_BYTES
                    && index_size(&cursor.parents).unwrap() <= MAX_INDEX_BYTES
            }));
        }
        assert_eq!(output, input);
        assert!(scanner
            .next(|_| async { bail!("unexpected fetch") })
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn scanner_crosses_data_pages_and_fence_leaves_for_hot_key() {
        let key = vec![b'h'; 150 * 1024];
        let input: Vec<_> = (0..4)
            .map(|block| Record {
                key: key.clone(),
                block,
                value: vec![block as u8; 400 * 1024],
            })
            .collect();
        let (reader, objects, _) = build(input.clone());
        let mut scanner = reader.scanner();
        let mut output = Vec::new();
        while let Some(record) = scanner
            .next(|reference| {
                let bytes = objects[&reference.digest].clone();
                async move { Ok(bytes) }
            })
            .await
            .unwrap()
        {
            output.push(record);
            let cursor = &scanner.cursors[partition(&key)];
            assert!(cursor.page.len() <= 1);
            assert!(cursor.leaf.len() <= 3);
        }
        assert_eq!(output, input);
        assert_eq!(scanner.cursors[partition(&key)].parent_pos, 2);
    }

    #[tokio::test]
    async fn scanner_rejects_corrupt_refs_and_cross_page_order() {
        let key = b"hot-key";
        let input: Vec<_> = (0..80)
            .map(|block| Record {
                key: key.to_vec(),
                block,
                value: vec![7; 24 * 1024],
            })
            .collect();
        let (reader, mut objects, _) = build(input);
        let shard = partition(key);
        let root = reader.indexes[shard];
        let mut fences = decode_index(root, &objects[&root.digest], shard, INDEX_MAGIC).unwrap();
        assert_eq!(fences.len(), 2);
        // Re-sign an index with a bad reference: the scanner must verify fetched bytes.
        let mut bad_reader = reader.clone();
        let mut bad_fences = fences.clone();
        bad_fences[0].data.digest = Hash32::digest(b"wrong");
        let bad_index = make_object(encode_index(&bad_fences, INDEX_MAGIC).unwrap()).unwrap();
        objects.insert(bad_index.reference.digest, bad_index.bytes);
        bad_reader.indexes[shard] = bad_index.reference;
        let mut scanner = bad_reader.scanner();
        assert!(scanner
            .next(|reference| {
                let bytes = objects[&if reference == bad_fences[0].data {
                    fences[0].data.digest
                } else {
                    reference.digest
                }]
                    .clone();
                async move { Ok(bytes) }
            })
            .await
            .is_err());

        // The second page is internally sorted and matches its exact fence, but
        // begins before the final record in the preceding page.
        let forged = Record {
            key: key.to_vec(),
            block: 1,
            value: vec![],
        };
        let mut payload = data_page_start();
        payload[5..9].copy_from_slice(&1_u32.to_be_bytes());
        payload.extend_from_slice(&encode_record(&forged).unwrap());
        let compressed = zstd::encode_all(payload.as_slice(), 3).unwrap();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(COMPRESSED_MAGIC);
        encoded.push(VERSION);
        encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&compressed);
        let page = make_object(encoded).unwrap();
        objects.insert(page.reference.digest, page.bytes);
        fences[1].block = 1;
        fences[1].data = page.reference;
        let index = make_object(encode_index(&fences, INDEX_MAGIC).unwrap()).unwrap();
        objects.insert(index.reference.digest, index.bytes);
        let mut forged_reader = reader;
        forged_reader.indexes[shard] = index.reference;
        let mut scanner = forged_reader.scanner();
        let mut seen = 0;
        loop {
            match scanner
                .next(|reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
            {
                Ok(Some(_)) => seen += 1,
                Err(_) => break,
                Ok(None) => panic!("out-of-order page accepted"),
            }
        }
        assert!(seen > 0);
        assert!(scanner
            .next(|_| async { bail!("unexpected fetch") })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn scanner_rejects_bad_leaf_first_key() {
        let key = vec![b'h'; 150 * 1024];
        let input: Vec<_> = (0..4)
            .map(|block| Record {
                key: key.clone(),
                block,
                value: vec![block as u8; 400 * 1024],
            })
            .collect();
        let (reader, mut objects, _) = build(input);
        let shard = partition(&key);
        let parent_ref = reader.indexes[shard];
        let parents = decode_index(
            parent_ref,
            &objects[&parent_ref.digest],
            shard,
            INTERNAL_MAGIC,
        )
        .unwrap();
        let leaf_ref = parents[1].data;
        let mut leaf =
            decode_index(leaf_ref, &objects[&leaf_ref.digest], shard, INDEX_MAGIC).unwrap();
        leaf[0].block -= 1;
        let forged_leaf = make_object(encode_index(&leaf, INDEX_MAGIC).unwrap()).unwrap();
        objects.insert(forged_leaf.reference.digest, forged_leaf.bytes);
        let mut forged_parents = parents;
        forged_parents[1].data = forged_leaf.reference;
        let forged_parent =
            make_object(encode_index(&forged_parents, INTERNAL_MAGIC).unwrap()).unwrap();
        objects.insert(forged_parent.reference.digest, forged_parent.bytes);
        let mut forged_reader = reader;
        forged_reader.indexes[shard] = forged_parent.reference;
        let mut scanner = forged_reader.scanner();
        for _ in 0..3 {
            assert!(scanner
                .next(|reference| {
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap()
                .is_some());
        }
        assert!(scanner
            .next(|reference| {
                let bytes = objects[&reference.digest].clone();
                async move { Ok(bytes) }
            })
            .await
            .is_err());
    }

    #[test]
    fn rejects_duplicate_or_unsorted_pairs() {
        let duplicate = records(b"k", &[(1, b"a"), (1, b"b")]);
        assert!(RunBuilder::build(duplicate, |_| Ok(())).is_err());
        let unsorted = vec![
            Record {
                key: b"z".to_vec(),
                block: 1,
                value: vec![],
            },
            Record {
                key: b"a".to_vec(),
                block: 1,
                value: vec![],
            },
        ];
        assert!(RunBuilder::build(unsorted, |_| Ok(())).is_err());
    }

    #[tokio::test]
    async fn verifies_complete_full_key_not_hash_route_alone() {
        let key = b"verified-key";
        let collision_route = (0_u64..)
            .map(|candidate| format!("zz-other-{candidate}"))
            .find(|candidate| partition(candidate.as_bytes()) == partition(key))
            .unwrap();
        let (reader, objects, _) = build(records(key, &[(3, b"v")]));
        let mut calls = 0;
        assert_eq!(
            reader
                .lookup(collision_route.as_bytes(), 3, |reference| {
                    calls += 1;
                    let bytes = objects[&reference.digest].clone();
                    async move { Ok(bytes) }
                })
                .await
                .unwrap(),
            None
        );
        assert_eq!(calls, 2);
    }

    #[test]
    fn verifies_digest_length_and_bounds() {
        let (reader, objects, _) = build(records(b"k", &[(1, b"v")]));
        let mut root_ref = reader.indexes[partition(b"k")];
        root_ref.length += 1;
        assert!(decode_index(
            root_ref,
            &objects[&reader.indexes[partition(b"k")].digest],
            partition(b"k"),
            INDEX_MAGIC,
        )
        .is_err());
        let oversized = vec![0; MAX_PAGE_BYTES + 1];
        assert!(decode_compressed(
            ObjectRef {
                digest: Hash32::digest(&oversized),
                length: oversized.len() as u32
            },
            &oversized,
            MAX_PAGE_BYTES,
        )
        .is_err());
    }

    #[test]
    fn streaming_build_caps_index_and_does_not_emit_a_root_on_overflow() {
        let key = vec![b'x'; 300 * 1024];
        let input = (0..8).map(|block| Record {
            key: key.clone(),
            block,
            value: vec![1],
        });
        let mut root_emitted = false;
        let result = RunBuilder::build(input, |object| {
            root_emitted |= object.bytes.starts_with(ROOT_MAGIC);
            Ok(())
        });
        assert!(result.is_err());
        assert!(!root_emitted);
    }
}
