//! Platform-neutral, bounded Fossil format-v1 reader used by the Worker and native tests.

use ruzstd::decoding::StreamingDecoder;
use sha2::{Digest as _, Sha256};
use sha3::Keccak256;
use std::collections::{HashMap, VecDeque};
use std::io::{Cursor as IoCursor, Read};

const VERSION: u8 = 1;
const HEAD_BYTES: usize = 97;
const COMMIT_BYTES: usize = 314;
const REF_BYTES: usize = 36;
const ROUTER_PARTITIONS: usize = 128;
const DATA_PARTITIONS: usize = 32;
const EPOCHS_PER_WINDOW: usize = 64;
const MAX_EPOCH_BLOCKS: u64 = 1_000;
const MAX_DIRECTORY_BYTES: usize = 2 * 1024 * 1024;
// Worker-specific cap; measured demo/real index deltas are far below this bound.
pub const MAX_INDEX_BYTES: usize = 2 * 1024 * 1024;
const MAX_INDEX_ENTRIES: usize = 65_536;
const MAX_INDEX_CANDIDATES: usize = 64;
// Worker-specific isolate bound. Native Fossil keeps its independent 64 MiB format cap.
pub const MAX_DATA_DECODED: usize = 8 * 1024 * 1024;
pub const MAX_DATA_OBJECT_BYTES: usize = MAX_DATA_DECODED + 256 * 1024;
const MAX_CODE_OBJECT_BYTES: usize = 1024 * 1024;
const MAX_REMOTE_GETS: u64 = 192;
const MAX_REQUEST_BYTES: u64 = 128 * 1024 * 1024;
const MAX_DECODED_BYTES: u64 = 128 * 1024 * 1024;
const CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
const CACHE_MAX_ITEMS: usize = 32;
const CACHE_MAX_ITEM_BYTES: usize = 2 * 1024 * 1024;
const EMPTY_CODE_HASH: [u8; 32] =
    hex_literal("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");

const fn hex_literal(value: &str) -> [u8; 32] {
    let bytes = value.as_bytes();
    let mut out = [0; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (hex_nibble(bytes[i * 2]) << 4) | hex_nibble(bytes[i * 2 + 1]);
        i += 1;
    }
    out
}
const fn hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => 0,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveError {
    Unavailable(String),
    Integrity,
    Limit,
    InvalidParams(String),
    Backend,
}

pub type Result<T> = std::result::Result<T, ArchiveError>;
fn integrity<T>() -> Result<T> {
    Err(ArchiveError::Integrity)
}

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ObjectRef {
    digest: [u8; 32],
    length: u32,
}
impl ObjectRef {
    fn empty(self) -> bool {
        self == Self::default()
    }
}

#[derive(Clone, Debug)]
pub struct Commit {
    pub generation: u64,
    pub chain_id: u64,
    pub anchor_number: u64,
    pub published_number: u64,
    pub finalized: bool,
    pub active_window_start: u64,
    active_directory: ObjectRef,
    published_hash: [u8; 32],
}

#[derive(Clone, Debug)]
struct Head {
    generation: u64,
    chain_id: u64,
    number: u64,
    hash: [u8; 32],
    commit: ObjectRef,
}
#[derive(Clone, Debug)]
struct Epoch {
    start: u64,
    end: u64,
    data: Vec<ObjectRef>,
    indexes: Vec<ObjectRef>,
}
#[derive(Clone, Debug)]
struct Directory {
    window_start: u64,
    base_checkpoint: ObjectRef,
    epochs: Vec<Epoch>,
}
#[derive(Clone, Copy, Debug)]
struct IndexCandidate {
    dictionary_index: u8,
    run_index: u32,
}
#[derive(Clone, Debug)]
struct IndexCandidates {
    partition: u8,
    epoch_start: u64,
    dictionary: Vec<ObjectRef>,
    candidates: Vec<IndexCandidate>,
}
#[derive(Clone, Debug)]
struct Version {
    value: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct Account {
    pub nonce: u64,
    pub balance: [u8; 32],
    incarnation: u64,
    code_hash: [u8; 32],
}

#[derive(Default)]
struct Budget {
    gets: u64,
    fetched: u64,
    decoded: u64,
}
impl Budget {
    fn reserve_fetch(&mut self, length: usize) -> Result<()> {
        let fetched = self
            .fetched
            .checked_add(length as u64)
            .ok_or(ArchiveError::Limit)?;
        if self.gets >= MAX_REMOTE_GETS || fetched > MAX_REQUEST_BYTES {
            return Err(ArchiveError::Limit);
        }
        self.gets += 1;
        self.fetched = fetched;
        Ok(())
    }
    fn decoded(&mut self, length: usize) -> Result<()> {
        self.decoded = self
            .decoded
            .checked_add(length as u64)
            .ok_or(ArchiveError::Limit)?;
        if self.decoded > MAX_DECODED_BYTES {
            return Err(ArchiveError::Limit);
        }
        Ok(())
    }
}

pub struct Reader<'a, S: ObjectStore> {
    store: &'a S,
    prefix: String,
    pub commit: Commit,
    budget: Budget,
    directory: Option<Directory>,
    cache: HashMap<String, Vec<u8>>,
    cache_order: VecDeque<String>,
    cache_bytes: usize,
}

impl<'a, S: ObjectStore> Reader<'a, S> {
    pub async fn load(store: &'a S, prefix: String, chain_id: u64) -> Result<Self> {
        let mut budget = Budget::default();
        let head_key = format!("{prefix}chains/0x{chain_id:x}/heads/finalized.bin");
        budget.reserve_fetch(HEAD_BYTES)?;
        let head_bytes = store.get(&head_key, HEAD_BYTES).await?;
        if head_bytes.len() != HEAD_BYTES {
            return integrity();
        }
        let head = parse_head(&head_bytes)?;
        if head.chain_id != chain_id || head.commit.length as usize != COMMIT_BYTES {
            return integrity();
        }
        let commit_bytes =
            get_verified(store, &prefix, head.commit, COMMIT_BYTES, &mut budget).await?;
        budget.decoded(COMMIT_BYTES)?;
        let commit = parse_commit(&commit_bytes)?;
        if commit.chain_id != chain_id
            || commit.generation != head.generation
            || commit.published_number != head.number
            || commit.published_hash != head.hash
        {
            return integrity();
        }
        Ok(Self {
            store,
            prefix,
            commit,
            budget,
            directory: None,
            cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_bytes: 0,
        })
    }

    pub fn resolve_selector(&self, selector: &serde_json::Value) -> Result<u64> {
        let selector = match selector {
            serde_json::Value::Object(object) => {
                if object
                    .keys()
                    .any(|key| key != "blockNumber" && key != "requireCanonical")
                {
                    return Err(ArchiveError::InvalidParams(
                        "invalid EIP-1898 selector".into(),
                    ));
                }
                let number = object.get("blockNumber").ok_or_else(|| {
                    ArchiveError::InvalidParams("block-hash selectors are not supported".into())
                })?;
                if object
                    .get("requireCanonical")
                    .is_some_and(|value| !value.is_boolean())
                {
                    return Err(ArchiveError::InvalidParams(
                        "requireCanonical must be boolean".into(),
                    ));
                }
                number
            }
            value => value,
        };
        let text = selector
            .as_str()
            .ok_or_else(|| ArchiveError::InvalidParams("invalid block selector".into()))?;
        let block = match text {
            "latest" => self.commit.published_number,
            "safe" | "finalized" => {
                if !self.commit.finalized {
                    return Err(ArchiveError::Unavailable(
                        "fixed-offset publication is not safe or finalized".into(),
                    ));
                }
                self.commit.published_number
            }
            "earliest" if self.commit.anchor_number == 0 => 0,
            "earliest" => {
                return Err(ArchiveError::Unavailable(
                    "earliest is before the archive anchor".into(),
                ))
            }
            "pending" => {
                return Err(ArchiveError::InvalidParams(
                    "pending is not supported".into(),
                ))
            }
            value => parse_quantity(value)?,
        };
        if block < self.commit.anchor_number || block > self.commit.published_number {
            return Err(ArchiveError::Unavailable(
                "block is outside the published archive range".into(),
            ));
        }
        if block < self.commit.active_window_start {
            return Err(ArchiveError::Unavailable(
                "completed-window lookup is not implemented by this Worker".into(),
            ));
        }
        Ok(block)
    }

    async fn get_uncached_object(
        &mut self,
        reference: ObjectRef,
        maximum: usize,
    ) -> Result<Vec<u8>> {
        get_verified(
            self.store,
            &self.prefix,
            reference,
            maximum,
            &mut self.budget,
        )
        .await
    }

    async fn get_object(&mut self, reference: ObjectRef, maximum: usize) -> Result<Vec<u8>> {
        validate_ref(reference, false)?;
        if reference.length as usize > maximum {
            return Err(ArchiveError::Limit);
        }
        let cache_key = format!("{}:{}", hex::encode(reference.digest), reference.length);
        if let Some(bytes) = self.cache.get(&cache_key) {
            return Ok(bytes.clone());
        }
        let bytes = get_verified(
            self.store,
            &self.prefix,
            reference,
            maximum,
            &mut self.budget,
        )
        .await?;
        if bytes.len() <= CACHE_MAX_ITEM_BYTES {
            while self.cache.len() >= CACHE_MAX_ITEMS
                || self.cache_bytes + bytes.len() > CACHE_MAX_BYTES
            {
                let Some(oldest) = self.cache_order.pop_front() else {
                    break;
                };
                if let Some(old) = self.cache.remove(&oldest) {
                    self.cache_bytes -= old.len();
                }
            }
            self.cache_bytes += bytes.len();
            self.cache_order.push_back(cache_key.clone());
            self.cache.insert(cache_key, bytes.clone());
        }
        Ok(bytes)
    }

    async fn ensure_directory(&mut self, block: u64) -> Result<Directory> {
        if block < self.commit.active_window_start {
            return Err(ArchiveError::Unavailable(
                "completed-window lookup is not implemented by this Worker".into(),
            ));
        }
        if let Some(directory) = &self.directory {
            return Ok(directory.clone());
        }
        let bytes = self
            .get_object(self.commit.active_directory, MAX_DIRECTORY_BYTES)
            .await?;
        self.budget.decoded(bytes.len())?;
        let directory = parse_directory(&bytes)?;
        let complete = directory_matches_publication(&directory, self.commit.published_number);
        if directory.window_start != self.commit.active_window_start || !complete {
            return integrity();
        }
        self.directory = Some(directory.clone());
        Ok(directory)
    }

    async fn lookup_raw(&mut self, key: &[u8], block: u64) -> Result<Option<Version>> {
        let directory = self.ensure_directory(block).await?;
        let (partition, fingerprint) = route_key(key)?;
        for epoch in directory.epochs.iter().rev() {
            if epoch.start > block {
                continue;
            }
            let index_ref = epoch.indexes[partition as usize];
            if index_ref.empty() {
                continue;
            }
            if index_ref.length as usize > MAX_INDEX_BYTES {
                return Err(ArchiveError::Limit);
            }
            let bytes = self.get_uncached_object(index_ref, MAX_INDEX_BYTES).await?;
            self.budget.decoded(bytes.len())?;
            let index = parse_index_candidates(&bytes, fingerprint, block)?;
            drop(bytes);
            if index.partition != partition || index.epoch_start != epoch.start {
                return integrity();
            }
            for entry in index.candidates {
                let data_ref = index.dictionary[entry.dictionary_index as usize];
                if data_ref != epoch.data[partition as usize >> 2] {
                    return integrity();
                }
                if data_ref.length as usize > MAX_DATA_OBJECT_BYTES {
                    return Err(ArchiveError::Limit);
                }
                let bytes = self.get_object(data_ref, MAX_DATA_OBJECT_BYTES).await?;
                if let Some(version) =
                    parse_data_lookup(&bytes, &mut self.budget, entry.run_index, key, block)?
                {
                    return Ok(Some(version));
                }
            }
        }
        if !directory.base_checkpoint.empty() {
            return Err(ArchiveError::Unavailable(
                "checkpoint fallback is not implemented by this Worker".into(),
            ));
        }
        Ok(None)
    }

    pub async fn account_at(&mut self, address: [u8; 20], block: u64) -> Result<Option<Account>> {
        let mut key = vec![1];
        key.extend_from_slice(&address);
        let Some(version) = self.lookup_raw(&key, block).await? else {
            return Ok(None);
        };
        decode_account(&version.value)
    }

    pub async fn storage_at(
        &mut self,
        address: [u8; 20],
        slot: [u8; 32],
        block: u64,
    ) -> Result<[u8; 32]> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok([0; 32]);
        };
        let mut key = vec![2];
        key.extend_from_slice(&address);
        key.extend_from_slice(&account.incarnation.to_be_bytes());
        key.extend_from_slice(&slot);
        match self.lookup_raw(&key, block).await? {
            Some(version) => decode_storage(&version.value),
            None => Ok([0; 32]),
        }
    }

    pub async fn code_at(&mut self, address: [u8; 20], block: u64) -> Result<Vec<u8>> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok(Vec::new());
        };
        if account.code_hash == EMPTY_CODE_HASH {
            return Ok(Vec::new());
        }
        let mut key = vec![3];
        key.extend_from_slice(&account.code_hash);
        let version = self
            .lookup_raw(&key, block)
            .await?
            .ok_or(ArchiveError::Integrity)?;
        let (first_seen, reference) = decode_code_meta(&version.value)?;
        if first_seen > block {
            return Ok(Vec::new());
        }
        let bytes = self.get_object(reference, MAX_CODE_OBJECT_BYTES).await?;
        charge_and_verify_code(bytes, account.code_hash, &mut self.budget)
    }
}

async fn get_verified<S: ObjectStore>(
    store: &S,
    prefix: &str,
    reference: ObjectRef,
    maximum: usize,
    budget: &mut Budget,
) -> Result<Vec<u8>> {
    validate_ref(reference, false)?;
    if reference.length as usize > maximum {
        return Err(ArchiveError::Limit);
    }
    let digest = hex::encode(reference.digest);
    let key = format!("{prefix}objects/sha256/{}/{digest}", &digest[..2]);
    budget.reserve_fetch(reference.length as usize)?;
    let bytes = store.get(&key, reference.length as usize).await?;
    if bytes.len() != reference.length as usize
        || Sha256::digest(&bytes).as_slice() != reference.digest
    {
        return integrity();
    }
    Ok(bytes)
}

fn parse_head(bytes: &[u8]) -> Result<Head> {
    let mut c = Cursor::new(bytes);
    c.expect(b"FSEH")?;
    c.version()?;
    let value = Head {
        generation: c.u64()?,
        chain_id: c.u64()?,
        number: c.u64()?,
        hash: c.hash()?,
        commit: c.object_ref()?,
    };
    c.end()?;
    validate_ref(value.commit, false)?;
    Ok(value)
}

fn parse_commit(bytes: &[u8]) -> Result<Commit> {
    if bytes.len() != COMMIT_BYTES {
        return integrity();
    }
    let mut c = Cursor::new(bytes);
    c.expect(b"FSEC")?;
    c.version()?;
    let generation = c.u64()?;
    let chain_id = c.u64()?;
    c.hash()?;
    let anchor_number = c.u64()?;
    c.hash()?;
    let published_hash = c.hash()?;
    let published_number = c.u64()?;
    c.hash()?;
    validate_ref(c.object_ref()?, true)?;
    let finalized = match c.byte()? {
        0 => false,
        1 => true,
        _ => return integrity(),
    };
    c.hash()?;
    let active_window_start = c.u64()?;
    let active_directory = c.object_ref()?;
    validate_ref(active_directory, false)?;
    validate_ref(c.object_ref()?, true)?;
    c.end()?;
    Ok(Commit {
        generation,
        chain_id,
        anchor_number,
        published_number,
        finalized,
        active_window_start,
        active_directory,
        published_hash,
    })
}

fn parse_directory(bytes: &[u8]) -> Result<Directory> {
    if bytes.len() > MAX_DIRECTORY_BYTES {
        return integrity();
    }
    let mut c = Cursor::new(bytes);
    c.expect(b"FSER")?;
    c.version()?;
    let window_start = c.u64()?;
    let base_checkpoint = c.object_ref()?;
    validate_ref(base_checkpoint, true)?;
    let count = c.byte()? as usize;
    if count > EPOCHS_PER_WINDOW {
        return integrity();
    }
    let mut epochs = Vec::with_capacity(count);
    let mut expected = window_start;
    for _ in 0..count {
        let start = c.u64()?;
        let end = c.u64()?;
        if start != expected || end < start || end - start + 1 > MAX_EPOCH_BLOCKS {
            return integrity();
        }
        validate_ref(c.object_ref()?, false)?;
        let mut data = Vec::with_capacity(DATA_PARTITIONS);
        for _ in 0..DATA_PARTITIONS {
            let r = c.object_ref()?;
            validate_ref(r, true)?;
            data.push(r);
        }
        let mut indexes = Vec::with_capacity(ROUTER_PARTITIONS);
        for _ in 0..ROUTER_PARTITIONS {
            let r = c.object_ref()?;
            validate_ref(r, true)?;
            indexes.push(r);
        }
        epochs.push(Epoch {
            start,
            end,
            data,
            indexes,
        });
        expected = end.checked_add(1).ok_or(ArchiveError::Integrity)?;
    }
    c.end()?;
    Ok(Directory {
        window_start,
        base_checkpoint,
        epochs,
    })
}

fn validate_index_entry_count(count: usize) -> Result<()> {
    if count > MAX_INDEX_ENTRIES {
        Err(ArchiveError::Limit)
    } else {
        Ok(())
    }
}

fn parse_index_candidates(
    bytes: &[u8],
    target_fingerprint: [u8; 12],
    target_block: u64,
) -> Result<IndexCandidates> {
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(ArchiveError::Limit);
    }
    let mut c = Cursor::new(bytes);
    c.expect(b"FSEI")?;
    c.version()?;
    let partition = c.byte()?;
    if partition as usize >= ROUTER_PARTITIONS {
        return integrity();
    }
    let epoch_start = c.u64()?;
    let dictionary_count = c.count(REF_BYTES)?;
    if dictionary_count > 4 {
        return integrity();
    }
    let mut dictionary = Vec::with_capacity(dictionary_count);
    for _ in 0..dictionary_count {
        let r = c.object_ref()?;
        validate_ref(r, false)?;
        dictionary.push(r);
    }
    let count = c.count(18)?;
    validate_index_entry_count(count)?;
    let mut candidates = Vec::new();
    let mut prior: Option<([u8; 12], u64, u32)> = None;
    for _ in 0..count {
        let fingerprint = c.array::<12>()?;
        let first_block = epoch_start
            .checked_add(c.uleb()?)
            .ok_or(ArchiveError::Integrity)?;
        let dictionary_index = c.byte()?;
        let run_index = c.u32()?;
        if dictionary_index as usize >= dictionary.len() {
            return integrity();
        }
        let ordering = (fingerprint, first_block, run_index);
        if prior.is_some_and(|prior| prior > ordering) {
            return integrity();
        }
        prior = Some(ordering);
        if fingerprint == target_fingerprint && first_block <= target_block {
            if candidates.len() >= MAX_INDEX_CANDIDATES {
                return Err(ArchiveError::Limit);
            }
            candidates.push(IndexCandidate {
                dictionary_index,
                run_index,
            });
        }
    }
    c.end()?;
    Ok(IndexCandidates {
        partition,
        epoch_start,
        dictionary,
        candidates,
    })
}

fn directory_matches_publication(directory: &Directory, published_number: u64) -> bool {
    if let Some(epoch) = directory.epochs.last() {
        epoch.end == published_number
    } else {
        published_number
            .checked_add(1)
            .is_some_and(|next| next == directory.window_start)
    }
}

fn validate_data_lengths(
    decoded_length: usize,
    encoded_length: usize,
    remaining: usize,
) -> Result<()> {
    if decoded_length > MAX_DATA_DECODED {
        return Err(ArchiveError::Limit);
    }
    if encoded_length != remaining {
        return integrity();
    }
    Ok(())
}

fn parse_data_lookup(
    bytes: &[u8],
    budget: &mut Budget,
    target_run: u32,
    target_key: &[u8],
    target_block: u64,
) -> Result<Option<Version>> {
    let mut c = Cursor::new(bytes);
    c.expect(b"FSED")?;
    c.version()?;
    let decoded_length = c.u32()? as usize;
    let encoded_length = c.u32()? as usize;
    validate_data_lengths(decoded_length, encoded_length, c.remaining())?;
    budget.decoded(decoded_length)?;
    let compressed = c.take(encoded_length)?;
    c.end()?;
    let decoder = StreamingDecoder::new_with_max_window_size(
        IoCursor::new(compressed),
        MAX_DATA_DECODED as u64,
    )
    .map_err(|_| ArchiveError::Integrity)?;
    let mut decoded = Vec::with_capacity(decoded_length.min(1024 * 1024));
    decoder
        .take(decoded_length as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(|_| ArchiveError::Integrity)?;
    if decoded.len() != decoded_length {
        return integrity();
    }

    // Keep only borrowed slices while validating the complete body. Peak owned data is
    // the encoded object, the <=8 MiB decoded buffer, and one selected value clone.
    let mut body = Cursor::new(&decoded);
    let count = body.count(3)?;
    if target_run as usize >= count {
        return integrity();
    }
    let mut prior_key: Option<&[u8]> = None;
    let mut selected: Option<&[u8]> = None;
    for run_index in 0..count {
        let key = body.bytes()?;
        if key.is_empty() || prior_key.is_some_and(|prior| prior >= key) {
            return integrity();
        }
        let version_count = body.count(2)?;
        if version_count == 0 {
            return integrity();
        }
        let mut block = 0u64;
        for version_index in 0..version_count {
            let next = block
                .checked_add(body.uleb()?)
                .ok_or(ArchiveError::Integrity)?;
            if version_index > 0 && next <= block {
                return integrity();
            }
            block = next;
            let value = body.bytes()?;
            if run_index == target_run as usize && key == target_key && block <= target_block {
                selected = Some(value);
            }
        }
        prior_key = Some(key);
    }
    body.end()?;
    Ok(selected.map(|value| Version {
        value: value.to_vec(),
    }))
}

fn charge_and_verify_code(
    bytes: Vec<u8>,
    expected_hash: [u8; 32],
    budget: &mut Budget,
) -> Result<Vec<u8>> {
    budget.decoded(bytes.len())?;
    if Keccak256::digest(&bytes).as_slice() != expected_hash {
        return integrity();
    }
    Ok(bytes)
}

fn decode_account(bytes: &[u8]) -> Result<Option<Account>> {
    let mut c = Cursor::new(bytes);
    let exists = c.byte()?;
    if exists > 1 {
        return integrity();
    }
    let incarnation = c.uleb()?;
    let nonce = c.uleb()?;
    let balance = c.trimmed_u256()?;
    let code_hash = c.hash()?;
    c.end()?;
    Ok((exists == 1).then_some(Account {
        nonce,
        balance,
        incarnation,
        code_hash,
    }))
}
fn decode_storage(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut c = Cursor::new(bytes);
    let value = c.trimmed_u256()?;
    c.end()?;
    Ok(value)
}
fn decode_code_meta(bytes: &[u8]) -> Result<(u64, ObjectRef)> {
    let mut c = Cursor::new(bytes);
    let first_seen = c.u64()?;
    let object = c.object_ref()?;
    validate_ref(object, false)?;
    if object.length as usize > MAX_CODE_OBJECT_BYTES {
        return integrity();
    }
    c.end()?;
    Ok((first_seen, object))
}

fn route_key(key: &[u8]) -> Result<(u8, [u8; 12])> {
    let full = Sha256::digest(key);
    let route = match key.first() {
        Some(1 | 2) if key.len() >= 21 => Sha256::digest(&key[1..21]),
        Some(3) if key.len() > 1 => Sha256::digest(&key[1..]),
        _ => return integrity(),
    };
    let mut fingerprint = [0; 12];
    fingerprint.copy_from_slice(&full[..12]);
    Ok((route[0] >> 1, fingerprint))
}
fn validate_ref(reference: ObjectRef, optional: bool) -> Result<()> {
    if optional && reference.empty() {
        return Ok(());
    }
    if reference.length == 0 || reference.digest == [0; 32] {
        return integrity();
    }
    Ok(())
}

pub fn parse_address(value: &serde_json::Value) -> Result<[u8; 20]> {
    let text = value
        .as_str()
        .ok_or_else(|| ArchiveError::InvalidParams("address must be 20-byte hex data".into()))?;
    if text.len() != 42 || !text.starts_with("0x") {
        return Err(ArchiveError::InvalidParams(
            "address must be 20-byte hex data".into(),
        ));
    }
    let bytes = hex::decode(&text[2..])
        .map_err(|_| ArchiveError::InvalidParams("address must be 20-byte hex data".into()))?;
    bytes
        .try_into()
        .map_err(|_| ArchiveError::InvalidParams("address must be 20-byte hex data".into()))
}
pub fn parse_slot(value: &serde_json::Value) -> Result<[u8; 32]> {
    let text = value.as_str().ok_or_else(|| {
        ArchiveError::InvalidParams("slot must be at most 32 bytes of hex".into())
    })?;
    if !text.starts_with("0x")
        || text.len() < 3
        || text.len() > 66
        || !text[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(ArchiveError::InvalidParams(
            "slot must be at most 32 bytes of hex".into(),
        ));
    }
    let mut digits = text[2..].to_string();
    if digits.len() % 2 == 1 {
        digits.insert(0, '0');
    }
    let bytes = hex::decode(digits)
        .map_err(|_| ArchiveError::InvalidParams("slot must be at most 32 bytes of hex".into()))?;
    let mut out = [0; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}
pub fn parse_quantity(value: &str) -> Result<u64> {
    let digits = value.strip_prefix("0x").ok_or_else(|| {
        ArchiveError::InvalidParams("block number must be a canonical hex quantity".into())
    })?;
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(ArchiveError::InvalidParams(
            "block number must be a canonical hex quantity".into(),
        ));
    }
    u64::from_str_radix(digits, 16)
        .map_err(|_| ArchiveError::InvalidParams("block number exceeds u64".into()))
}
pub fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}
pub fn quantity_bytes(value: &[u8; 32]) -> String {
    let text = hex::encode(value).trim_start_matches('0').to_owned();
    format!("0x{}", if text.is_empty() { "0" } else { &text })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(ArchiveError::Integrity)?;
        if end > self.bytes.len() {
            return integrity();
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| ArchiveError::Integrity)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<()> {
        if self.take(expected.len())? != expected {
            return integrity();
        }
        Ok(())
    }
    fn version(&mut self) -> Result<()> {
        if self.byte()? != VERSION {
            return integrity();
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn hash(&mut self) -> Result<[u8; 32]> {
        self.array()
    }
    fn object_ref(&mut self) -> Result<ObjectRef> {
        Ok(ObjectRef {
            digest: self.hash()?,
            length: self.u32()?,
        })
    }
    fn uleb(&mut self) -> Result<u64> {
        let start = self.offset;
        let mut result = 0u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return integrity();
            }
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                let mut encoded = Vec::new();
                put_uleb(&mut encoded, result);
                if self.bytes[start..self.offset] != encoded {
                    return integrity();
                }
                return Ok(result);
            }
        }
        integrity()
    }
    fn count(&mut self, minimum: usize) -> Result<usize> {
        let count = usize::try_from(self.uleb()?).map_err(|_| ArchiveError::Integrity)?;
        if count > self.remaining() / minimum {
            return integrity();
        }
        Ok(count)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let count = usize::try_from(self.uleb()?).map_err(|_| ArchiveError::Integrity)?;
        self.take(count)
    }
    fn trimmed_u256(&mut self) -> Result<[u8; 32]> {
        let length = self.byte()? as usize;
        if length > 32 {
            return integrity();
        }
        let bytes = self.take(length)?;
        if bytes.first() == Some(&0) {
            return integrity();
        }
        let mut out = [0; 32];
        out[32 - length..].copy_from_slice(bytes);
        Ok(out)
    }
    fn end(&self) -> Result<()> {
        if self.remaining() != 0 {
            return integrity();
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::cell::Cell;

    struct CountingStore(Cell<usize>);
    impl ObjectStore for CountingStore {
        async fn get(&self, _key: &str, _maximum: usize) -> Result<Vec<u8>> {
            self.0.set(self.0.get() + 1);
            Err(ArchiveError::Unavailable("missing".into()))
        }
    }

    fn reader(store: &CountingStore) -> Reader<'_, CountingStore> {
        Reader {
            store,
            prefix: "fixture/".into(),
            commit: Commit {
                generation: 1,
                chain_id: 1,
                anchor_number: 0,
                published_number: 0,
                finalized: true,
                active_window_start: 0,
                active_directory: ObjectRef {
                    digest: [1; 32],
                    length: 1,
                },
                published_hash: [2; 32],
            },
            budget: Budget::default(),
            directory: None,
            cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_bytes: 0,
        }
    }

    #[test]
    fn worker_data_caps_have_checked_boundaries_and_reject_before_get() {
        assert_eq!(MAX_DATA_DECODED, 8 * 1024 * 1024);
        assert_eq!(MAX_DATA_OBJECT_BYTES, MAX_DATA_DECODED + 256 * 1024);
        assert!(validate_data_lengths(MAX_DATA_DECODED, 12, 12).is_ok());
        assert_eq!(
            validate_data_lengths(MAX_DATA_DECODED + 1, 0, 0),
            Err(ArchiveError::Limit)
        );

        block_on(async {
            let store = CountingStore(Cell::new(0));
            let mut bounded_reader = reader(&store);
            let oversized = ObjectRef {
                digest: [3; 32],
                length: (MAX_DATA_OBJECT_BYTES + 1) as u32,
            };
            assert_eq!(
                bounded_reader
                    .get_object(oversized, MAX_DATA_OBJECT_BYTES)
                    .await,
                Err(ArchiveError::Limit)
            );
            assert_eq!(store.0.get(), 0);

            let boundary = ObjectRef {
                digest: [4; 32],
                length: MAX_DATA_OBJECT_BYTES as u32,
            };
            assert!(matches!(
                bounded_reader
                    .get_object(boundary, MAX_DATA_OBJECT_BYTES)
                    .await,
                Err(ArchiveError::Unavailable(_))
            ));
            assert_eq!(store.0.get(), 1);

            let exhausted_store = CountingStore(Cell::new(0));
            let mut exhausted = reader(&exhausted_store);
            exhausted.budget.fetched = MAX_REQUEST_BYTES;
            let small = ObjectRef {
                digest: [5; 32],
                length: 1,
            };
            assert_eq!(
                exhausted.get_object(small, MAX_DATA_OBJECT_BYTES).await,
                Err(ArchiveError::Limit)
            );
            assert_eq!(exhausted_store.0.get(), 0);
        });
    }

    fn index_with_candidates(count: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"FSEI");
        bytes.push(VERSION);
        bytes.push(0);
        bytes.extend_from_slice(&0u64.to_be_bytes());
        put_uleb(&mut bytes, 1);
        bytes.extend_from_slice(&[9; 32]);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        put_uleb(&mut bytes, count as u64);
        for run_index in 0..count {
            bytes.extend_from_slice(&[7; 12]);
            put_uleb(&mut bytes, 0);
            bytes.push(0);
            bytes.extend_from_slice(&(run_index as u32).to_be_bytes());
        }
        bytes
    }

    #[test]
    fn worker_index_cap_and_candidate_cpu_bounds_fail_closed() {
        block_on(async {
            let store = CountingStore(Cell::new(0));
            let mut bounded_reader = reader(&store);
            let oversized = ObjectRef {
                digest: [6; 32],
                length: (MAX_INDEX_BYTES + 1) as u32,
            };
            assert_eq!(
                bounded_reader
                    .get_uncached_object(oversized, MAX_INDEX_BYTES)
                    .await,
                Err(ArchiveError::Limit)
            );
            assert_eq!(store.0.get(), 0);

            let boundary = ObjectRef {
                digest: [7; 32],
                length: MAX_INDEX_BYTES as u32,
            };
            assert!(matches!(
                bounded_reader
                    .get_uncached_object(boundary, MAX_INDEX_BYTES)
                    .await,
                Err(ArchiveError::Unavailable(_))
            ));
            assert_eq!(store.0.get(), 1);
        });

        assert!(validate_index_entry_count(MAX_INDEX_ENTRIES).is_ok());
        assert_eq!(
            validate_index_entry_count(MAX_INDEX_ENTRIES + 1),
            Err(ArchiveError::Limit)
        );
        assert!(matches!(
            parse_index_candidates(&index_with_candidates(MAX_INDEX_CANDIDATES + 1), [7; 12], 0),
            Err(ArchiveError::Limit)
        ));
        assert_eq!(
            parse_index_candidates(&index_with_candidates(MAX_INDEX_CANDIDATES), [7; 12], 0)
                .unwrap()
                .candidates
                .len(),
            MAX_INDEX_CANDIDATES
        );
    }

    #[test]
    fn fetched_bytes_are_reserved_atomically_before_io() {
        let mut head_budget = Budget::default();
        assert!(head_budget.reserve_fetch(HEAD_BYTES).is_ok());
        assert_eq!(
            (head_budget.gets, head_budget.fetched),
            (1, HEAD_BYTES as u64)
        );

        let mut budget = Budget {
            gets: 7,
            fetched: MAX_REQUEST_BYTES - 4,
            decoded: 0,
        };
        assert_eq!(budget.reserve_fetch(5), Err(ArchiveError::Limit));
        assert_eq!((budget.gets, budget.fetched), (7, MAX_REQUEST_BYTES - 4));
        assert!(budget.reserve_fetch(4).is_ok());
        assert_eq!((budget.gets, budget.fetched), (8, MAX_REQUEST_BYTES));
    }

    #[test]
    fn code_is_charged_before_keccak_and_account_tombstones_preserve_incarnation_codec() {
        let mut budget = Budget {
            gets: 0,
            fetched: 0,
            decoded: MAX_DECODED_BYTES,
        };
        assert_eq!(
            charge_and_verify_code(vec![0xaa], [0; 32], &mut budget),
            Err(ArchiveError::Limit)
        );

        let mut live = vec![1, 7, 3, 0];
        live.extend_from_slice(&EMPTY_CODE_HASH);
        let account = decode_account(&live).unwrap().unwrap();
        assert_eq!(account.incarnation, 7);
        assert_eq!(account.nonce, 3);

        live[0] = 0;
        assert!(decode_account(&live).unwrap().is_none());
    }

    #[test]
    fn empty_directory_requires_checked_published_successor() {
        let empty = Directory {
            window_start: 11,
            base_checkpoint: ObjectRef::default(),
            epochs: vec![],
        };
        assert!(directory_matches_publication(&empty, 10));
        let wrapped = Directory {
            window_start: 0,
            base_checkpoint: ObjectRef::default(),
            epochs: vec![],
        };
        assert!(!directory_matches_publication(&wrapped, u64::MAX));
    }
}
