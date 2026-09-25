//! Immutable time-disjoint runs with head-last publication.
//!
//! The mutable head carries the whole manifest inline (run intervals, run roots
//! and summary shard counts), so a cold reader needs one GET before probing
//! runs. Publication only appends an L0 run; `compact_once` separately folds the
//! oldest `L0_TRIGGER` L0 runs, cascading through levels, and commits by CAS
//! against whatever head the publisher has advanced to meanwhile.

use crate::compact::{fetch, merge_runs, write_sorted, BuiltRun};
use crate::format::{AccountEvent, Address, Hash32};
use crate::normalized::{Mode, Package};
use crate::run::{Record, RunReader};
use crate::store::{ArchiveStore, VersionedBytes};
use crate::summary::{shard_path, Probe, Shard, ShardKind, MAX_SHARD_BYTES};
use anyhow::{bail, Context, Result};
use fossil_codec::head::{
    account_key, code_key, head_key, storage_key, unit, AccountState, Head, Ref, Run, FANOUT,
    MAX_HEAD, MAX_LEVELS,
};
pub use fossil_codec::head::{L0_TRIGGER, MAX_L0_BACKLOG};
use futures_util::TryStreamExt;
use moka::sync::Cache;
use sha3::{Digest, Keccak256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

/// Canonicality evidence for the last block of a package being published.
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

const MAX_STAGED: usize = 256 * 1024 * 1024;
const MAX_CODE: usize = 1024 * 1024;
const CAS_ATTEMPTS: usize = 32;
const MAX_REQUEST_GETS: u32 = 192;
const MAX_REQUEST_BYTES: u64 = 128 * 1024 * 1024;
/// Bound on memoized summary shards held by one pinned reader.
const SHARD_CACHE_BYTES: u64 = 256 * 1024 * 1024;

async fn stage(store: &dyn ArchiveStore, bytes: &[u8]) -> Result<Ref> {
    let reference = Ref {
        digest: Hash32::digest(bytes),
        length: u32::try_from(bytes.len())?,
    };
    store
        .put_immutable(&reference.digest.object_key(), bytes)
        .await?;
    Ok(reference)
}

async fn read_head(
    store: &dyn ArchiveStore,
    chain_id: u64,
) -> Result<Option<(VersionedBytes, Head)>> {
    let Some(bytes) = store
        .read_mutable_bounded(&head_key(chain_id), MAX_HEAD)
        .await?
    else {
        return Ok(None);
    };
    let head = Head::decode(&bytes.bytes)?;
    if head.chain_id != chain_id {
        bail!("tiered head chain mismatch");
    }
    Ok(Some((bytes, head)))
}

/// CAS `next` over `previous`, recording `previous` as the immutable parent.
async fn swap(
    store: &dyn ArchiveStore,
    previous: Option<&VersionedBytes>,
    mut next: Head,
) -> Result<()> {
    next.parent = match previous {
        Some(previous) => Some(stage(store, &previous.bytes).await?),
        None => None,
    };
    let bytes = serde_json::to_vec(&next)?;
    if bytes.len() > MAX_HEAD {
        bail!("tiered head exceeds its size bound");
    }
    store
        .compare_and_swap(&head_key(next.chain_id), previous, &bytes)
        .await
}

fn account_value(event: &AccountEvent) -> Vec<u8> {
    AccountState {
        exists: event.exists,
        incarnation: event.incarnation,
        nonce: event.nonce,
        balance: event.balance,
        code_hash: event.code_hash.0,
    }
    .encode()
}

fn new_run(start: u64, end: u64, weight: u64, built: BuiltRun) -> Run {
    Run {
        start,
        end,
        weight,
        keys: built.keys,
        shards: built.shards,
        root: built.root.into(),
        root_bytes: hex::encode(built.root_bytes),
    }
}

fn package_records(package: &Package) -> Result<Vec<Record>> {
    let segment = &package.segment;
    let (first, last) = (
        segment.blocks.first().context("empty package")?,
        segment.blocks.last().unwrap(),
    );
    let mut records = Vec::new();
    let mut input_bytes = 0_usize;
    let mut add = |key: Vec<u8>, block: u64, value: Vec<u8>| -> Result<()> {
        if !(first.number..=last.number).contains(&block) {
            bail!("event outside package interval");
        }
        input_bytes = input_bytes
            .checked_add(key.len() + value.len() + 32)
            .context("input byte overflow")?;
        if input_bytes > MAX_STAGED {
            bail!("tiered in-memory input exceeds 256 MiB");
        }
        records.push(Record { key, block, value });
        Ok(())
    };
    for event in &segment.accounts {
        add(
            account_key(event.address.0),
            event.block,
            account_value(event),
        )?;
    }
    for event in &segment.storage {
        add(
            storage_key(event.address.0, event.incarnation, event.slot.0),
            event.block,
            event.value.to_vec(),
        )?;
    }
    for blob in &segment.code {
        if blob.bytes.len() > MAX_CODE
            || Hash32(Keccak256::digest(&blob.bytes).into()) != blob.code_hash
        {
            bail!("invalid or oversized code blob");
        }
        add(
            code_key(blob.code_hash.0),
            blob.first_seen_block,
            blob.bytes.clone(),
        )?;
    }
    records.sort_unstable_by(|a, b| (&a.key, a.block).cmp(&(&b.key, b.block)));
    Ok(records)
}

/// In-package lifecycle checks, run before any upload. An anchor is exhaustive, so
/// every storage record and code reference must be established inside it. In a
/// delta, storage of an address with an account record at or before the same
/// block must use that record's incarnation.
// ponytail: storage of addresses without an in-package account record, and code
// reused from earlier packages, rely on the exporter's lifetime journal; checking
// them here would cost reader GETs per touched address.
fn validate_lifecycle(package: &Package) -> Result<()> {
    let segment = &package.segment;
    let mut accounts: HashMap<Address, Vec<&AccountEvent>> = HashMap::new();
    for event in &segment.accounts {
        accounts.entry(event.address).or_default().push(event);
    }
    for events in accounts.values_mut() {
        events.sort_by_key(|event| event.block);
    }
    let anchor = matches!(package.mode, Mode::Anchor);
    for event in &segment.storage {
        let account = accounts.get(&event.address).and_then(|events| {
            events
                .iter()
                .rev()
                .find(|account| account.block <= event.block)
        });
        match account {
            Some(account) if account.exists && account.incarnation == event.incarnation => {}
            Some(account) if !account.exists && event.value == [0; 32] => {}
            None if !anchor => {}
            _ => bail!("storage record does not match its account incarnation"),
        }
    }
    if anchor {
        let codes: HashSet<Hash32> = segment.code.iter().map(|blob| blob.code_hash).collect();
        let empty = Hash32(Keccak256::digest([]).into());
        if segment.accounts.iter().any(|account| {
            account.exists && account.code_hash != empty && !codes.contains(&account.code_hash)
        }) {
            bail!("anchor account references code absent from the package");
        }
    }
    Ok(())
}

fn is_published(head: &Head, package: &Package) -> bool {
    let segment = &package.segment;
    segment.blocks.last().is_some_and(|last| {
        head.chain_id == segment.chain_id
            && head.genesis == segment.genesis_hash
            && head.number == last.number
            && head.hash == last.hash
            && head.input_sha256 == package.input_sha256
    })
}

/// Append one sealed package as an L0 run. Never compacts; fails with a backlog
/// error when `MAX_L0_BACKLOG` runs await `compact_once`. All objects are durable
/// before the head CAS; failures can leave only unreachable objects.
/// Callers must pass an already normalized package and an observed canonical gate.
pub async fn publish(
    store: &dyn ArchiveStore,
    package: &Package,
    gate: &PublicationGate,
) -> Result<()> {
    let segment = &package.segment;
    let (first, last) = (
        segment.blocks.first().context("empty package")?,
        segment.blocks.last().unwrap(),
    );
    if segment.start_block != first.number
        || segment.end_block != last.number
        || segment.blocks.len() > 1000
        || segment.blocks.windows(2).any(|b| {
            b[0].number.checked_add(1) != Some(b[1].number) || b[0].hash != b[1].parent_hash
        })
    {
        bail!("invalid package block interval");
    }
    match gate {
        PublicationGate::Finalized { number, hash }
            if *number == last.number && *hash == last.hash => {}
        PublicationGate::FixedOffset {
            observed_number,
            observed_hash,
            offset: 0,
        } if *observed_number == last.number && *observed_hash == last.hash => {}
        _ => bail!("tiered publication gate is not canonical at the package end"),
    }
    let mut previous = read_head(store, segment.chain_id).await?;
    if previous
        .as_ref()
        .is_some_and(|(_, head)| is_published(head, package))
    {
        return Ok(());
    }
    match (&package.mode, &previous) {
        (Mode::Anchor, None)
            if first.number == 0
                && first.hash == segment.genesis_hash
                && first.parent_hash == Hash32([0; 32]) => {}
        (Mode::Delta, Some((_, head)))
            if head.genesis == segment.genesis_hash
                && head.number.checked_add(1) == Some(first.number)
                && package.preceding_number == Some(head.number)
                && package.preceding_hash == Some(head.hash)
                && first.parent_hash == head.hash => {}
        _ => bail!("tiered chain/genesis/parent transition invalid"),
    }
    if previous
        .as_ref()
        .is_some_and(|(_, head)| head.l0.len() >= MAX_L0_BACKLOG)
    {
        bail!("tiered L0 compaction backlog is full");
    }
    validate_lifecycle(package)?;
    let built = write_sorted(store, package_records(package)?).await?;
    let run = new_run(first.number, last.number, 1, built);
    for _ in 0..CAS_ATTEMPTS {
        let next = match &previous {
            Some((_, head)) => {
                let mut next = head.clone();
                next.l0.push(run.clone());
                next
            }
            None => Head {
                magic: "FTVH1".into(),
                chain_id: segment.chain_id,
                genesis: segment.genesis_hash,
                number: 0,
                hash: first.hash,
                state_root: first.state_root,
                input_sha256: package.input_sha256,
                parent: None,
                l0: vec![run.clone()],
                levels: Vec::new(),
            },
        };
        let next = Head {
            number: last.number,
            hash: last.hash,
            state_root: last.state_root,
            input_sha256: package.input_sha256,
            ..next
        };
        let expected = previous.as_ref().map(|(bytes, _)| bytes);
        let Err(error) = swap(store, expected, next).await else {
            return Ok(());
        };
        let current = read_head(store, segment.chain_id).await?;
        match (&current, &previous) {
            (Some((_, now)), _) if is_published(now, package) => return Ok(()),
            // A compactor replaced runs under the same chain head: rebase.
            (Some((_, now)), Some((_, before)))
                if now.number == before.number && now.hash == before.hash =>
            {
                previous = current;
            }
            _ => return Err(error),
        }
    }
    bail!("tiered publication lost too many CAS races")
}

/// Fold the oldest `L0_TRIGGER` L0 runs (cascading through full levels) into one
/// run with a single streaming k-way merge. Returns whether a head was committed.
/// Safe to run concurrently with `publish`, which only appends L0 runs.
pub async fn compact_once(store: &dyn ArchiveStore, chain_id: u64) -> Result<bool> {
    let Some((_, planned)) = read_head(store, chain_id).await? else {
        return Ok(false);
    };
    if planned.l0.len() < L0_TRIGGER {
        return Ok(false);
    }
    let mut levels = planned.levels.clone();
    let mut weight = L0_TRIGGER as u64;
    let mut deeper = Vec::new();
    let mut target = 0;
    loop {
        if target >= MAX_LEVELS {
            bail!("tiered history exceeds the bounded level count");
        }
        if levels.len() == target {
            levels.push(None);
        }
        if let Some(run) = levels[target].take() {
            weight += run.weight;
            deeper.push(run);
        }
        if weight < unit(target) * FANOUT {
            break;
        }
        if weight != unit(target) * FANOUT {
            bail!("tiered level capacity exceeded");
        }
        target += 1;
    }
    let inputs: Vec<Run> = deeper
        .into_iter()
        .rev()
        .chain(planned.l0[..L0_TRIGGER].iter().cloned())
        .collect();
    if inputs
        .windows(2)
        .any(|pair| pair[0].end.checked_add(1) != Some(pair[1].start))
    {
        bail!("noncontiguous tiered compaction inputs");
    }
    let refs = inputs.iter().map(Run::input).collect::<Result<Vec<_>>>()?;
    let expected: u64 = inputs.iter().map(|run| run.keys).sum();
    let built = merge_runs(store, &refs, expected).await?;
    levels[target] = Some(new_run(
        inputs[0].start,
        inputs.last().unwrap().end,
        weight,
        built,
    ));
    while levels.last().is_some_and(Option::is_none) {
        levels.pop();
    }
    for _ in 0..CAS_ATTEMPTS {
        let (bytes, current) = read_head(store, chain_id)
            .await?
            .context("tiered head vanished")?;
        if current.levels != planned.levels || !current.l0.starts_with(&planned.l0[..L0_TRIGGER]) {
            bail!("tiered head changed incompatibly during compaction");
        }
        let next = Head {
            l0: current.l0[L0_TRIGGER..].to_vec(),
            levels: levels.clone(),
            ..current
        };
        if swap(store, Some(&bytes), next).await.is_ok() {
            return Ok(true);
        }
    }
    bail!("tiered compaction lost too many CAS races")
}

/// Immutable snapshot pinned to the head read at open time. Fetched summary
/// shards are memoized (bounded) so account and storage probes of one address
/// share GETs.
pub struct Reader<'a> {
    store: &'a dyn ArchiveStore,
    head: Head,
    head_digest: Hash32,
    runs: Vec<(Run, RunReader)>,
    shards: Cache<String, Arc<Shard>>,
}

/// Outcome of one garbage-collection pass.
#[derive(Debug, Default, serde::Serialize)]
pub struct GcReport {
    pub live_objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
    /// Unreachable objects kept because they are younger than the grace age.
    pub kept_young: usize,
}

/// Delete immutable objects unreachable from the current head: superseded runs'
/// pages, fences, roots and summary shards, plus head copies beyond the newest
/// `keep_heads`. Only objects older than `min_age` are deleted, and nothing is
/// deleted if the head changes during the pass.
///
/// Run with publishers and compactors stopped: a writer can re-reference an old
/// unreachable object by content address, which a concurrent sweep could delete.
/// Readers pinned to a superseded head may fail once its runs are gone.
pub async fn collect_garbage(
    store: &dyn ArchiveStore,
    chain_id: u64,
    keep_heads: usize,
    min_age: std::time::Duration,
    dry_run: bool,
) -> Result<GcReport> {
    let started = std::time::SystemTime::now();
    let (before, head) = read_head(store, chain_id)
        .await?
        .context("missing tiered head")?;
    let mut live = HashSet::new();
    for run in head.runs() {
        let (root, bytes) = run.input()?;
        live.insert(root.digest.object_key());
        let refs = RunReader::decode(root, &bytes)?
            .object_refs(|reference| fetch(store, reference))
            .await?;
        live.extend(
            refs.into_iter()
                .map(|reference| reference.digest.object_key()),
        );
        for kind in [ShardKind::Address, ShardKind::Spill] {
            live.extend(
                (0..run.shards).map(|index| shard_path(run.root.digest, run.shards, kind, index)),
            );
        }
    }
    let mut parent = head.parent;
    for _ in 0..keep_heads {
        let Some(reference) = parent else { break };
        let key = reference.digest.object_key();
        let Ok(bytes) = store.get_bounded(&key, MAX_HEAD).await else {
            break;
        };
        live.insert(key);
        parent = serde_json::from_slice::<Head>(&bytes)
            .ok()
            .and_then(|head| head.parent);
    }
    let cutoff = started - min_age;
    let mut report = GcReport {
        live_objects: live.len(),
        ..GcReport::default()
    };
    let mut doomed = Vec::new();
    for listed in [
        store.list("objects/").await?,
        store.list("summaries/").await?,
    ]
    .concat()
    {
        if live.contains(&listed.key) {
            continue;
        }
        if listed.modified > cutoff {
            report.kept_young += 1;
            continue;
        }
        report.deleted_objects += 1;
        report.deleted_bytes += listed.size;
        doomed.push(listed.key);
    }
    let (after, _) = read_head(store, chain_id)
        .await?
        .context("tiered head vanished")?;
    if after.bytes != before.bytes {
        bail!("tiered head changed during garbage collection; stop writers and retry");
    }
    if !dry_run {
        futures_util::stream::iter(doomed.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(32, |key| async move { store.delete(&key).await })
            .await?;
    }
    Ok(report)
}

/// Hard per-request resource limits, charged with each object's maximum size
/// before its GET is issued. Memoized summary shards are free.
#[derive(Default)]
struct Budget {
    gets: AtomicU32,
    bytes: AtomicU64,
}
impl Budget {
    fn charge(&self, maximum: usize) -> Result<()> {
        let gets = self.gets.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes = self.bytes.fetch_add(maximum as u64, Ordering::Relaxed) + maximum as u64;
        if gets > MAX_REQUEST_GETS || bytes > MAX_REQUEST_BYTES {
            bail!("tiered read exceeds its per-request GET or byte budget");
        }
        Ok(())
    }
}
impl<'a> Reader<'a> {
    pub async fn open(store: &'a dyn ArchiveStore, chain_id: u64) -> Result<Self> {
        let (bytes, head) = read_head(store, chain_id)
            .await?
            .context("missing tiered head")?;
        let mut runs = Vec::new();
        for run in head.runs() {
            let (reference, bytes) = run.input()?;
            runs.push((run.clone(), RunReader::decode(reference, &bytes)?));
        }
        Ok(Self {
            store,
            head_digest: Hash32::digest(&bytes.bytes),
            head,
            runs,
            shards: Cache::builder()
                .weigher(|_, shard: &Arc<Shard>| shard.len() as u32)
                .max_capacity(SHARD_CACHE_BYTES)
                .build(),
        })
    }
    pub fn latest_block(&self) -> u64 {
        self.head.number
    }
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }
    /// Uncompacted L0 runs in the pinned head.
    pub fn l0_count(&self) -> usize {
        self.head.l0.len()
    }
    pub fn chain_id(&self) -> u64 {
        self.head.chain_id
    }
    /// SHA-256 of the exact head bytes this reader pinned.
    pub fn head_digest(&self) -> Hash32 {
        self.head_digest
    }
    async fn shard(
        &self,
        budget: &Budget,
        run: &Run,
        kind: ShardKind,
        index: u32,
    ) -> Result<Arc<Shard>> {
        let path = shard_path(run.root.digest, run.shards, kind, index);
        if let Some(shard) = self.shards.get(&path) {
            return Ok(shard);
        }
        budget.charge(MAX_SHARD_BYTES)?;
        let bytes = self.store.get_bounded(&path, MAX_SHARD_BYTES).await?;
        let shard = Arc::new(Shard::decode(
            run.root.digest,
            kind,
            index,
            run.shards,
            &bytes,
        )?);
        self.shards.insert(path, shard.clone());
        Ok(shard)
    }
    async fn may_contain(&self, budget: &Budget, run: &Run, key: &[u8]) -> Result<bool> {
        let probe = Probe::new(key, run.shards);
        let address = self
            .shard(budget, run, ShardKind::Address, probe.address_index)
            .await?;
        if address.contains(key) {
            return Ok(true);
        }
        if !address.contains(&probe.marker) {
            return Ok(false);
        }
        Ok(self
            .shard(budget, run, ShardKind::Spill, probe.spill_index)
            .await?
            .contains(key))
    }
    async fn lookup(&self, budget: &Budget, key: &[u8], block: u64) -> Result<Option<Vec<u8>>> {
        if block > self.head.number {
            bail!("block outside pinned tiered history");
        }
        for (run, reader) in self.runs.iter().rev() {
            if run.start > block || !self.may_contain(budget, run, key).await? {
                continue;
            }
            let value = reader
                .lookup(key, block.min(run.end), |reference| async move {
                    budget.charge(reference.length as usize)?;
                    fetch(self.store, reference).await
                })
                .await?;
            if value.is_some() {
                return Ok(value);
            }
        }
        Ok(None)
    }
    pub async fn account(&self, address: Address, block: u64) -> Result<Option<AccountEvent>> {
        self.account_in(&Budget::default(), address, block).await
    }
    async fn account_in(
        &self,
        budget: &Budget,
        address: Address,
        block: u64,
    ) -> Result<Option<AccountEvent>> {
        let Some(value) = self.lookup(budget, &account_key(address.0), block).await? else {
            return Ok(None);
        };
        let state = AccountState::decode(&value)?;
        Ok(Some(AccountEvent {
            block,
            address,
            exists: state.exists,
            incarnation: state.incarnation,
            nonce: state.nonce,
            balance: state.balance,
            code_hash: Hash32(state.code_hash),
        }))
    }
    pub async fn storage(&self, address: Address, slot: Hash32, block: u64) -> Result<[u8; 32]> {
        let budget = Budget::default();
        let Some(account) = self.account_in(&budget, address, block).await? else {
            return Ok([0; 32]);
        };
        if !account.exists {
            return Ok([0; 32]);
        }
        let value = self
            .lookup(
                &budget,
                &storage_key(address.0, account.incarnation, slot.0),
                block,
            )
            .await?;
        match value {
            Some(value) => Ok(value
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid full-key storage poststate"))?),
            None => Ok([0; 32]),
        }
    }
    pub async fn code(&self, address: Address, block: u64) -> Result<Vec<u8>> {
        let budget = Budget::default();
        let Some(account) = self.account_in(&budget, address, block).await? else {
            return Ok(Vec::new());
        };
        if !account.exists {
            return Ok(Vec::new());
        }
        let empty = Hash32(Keccak256::digest([]).into());
        if account.code_hash == empty {
            return Ok(Vec::new());
        }
        let bytes = self
            .lookup(&budget, &code_key(account.code_hash.0), block)
            .await?
            .context("missing account code blob")?;
        if bytes.len() > MAX_CODE || Hash32(Keccak256::digest(&bytes).into()) != account.code_hash {
            bail!("invalid code poststate");
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::genesis_anchor;
    use crate::format::{BlockMeta, CodeBlob, Segment, StorageEvent, SEGMENT_SCHEMA};
    use crate::normalized::read_package;
    use crate::store::MemoryArchiveStore;
    use serde_json::json;
    use std::sync::atomic::Ordering;

    fn h(byte: u8) -> Hash32 {
        Hash32([byte; 32])
    }
    fn gate(number: u64, hash: Hash32) -> PublicationGate {
        PublicationGate::Finalized { number, hash }
    }
    fn delta(
        number: u64,
        parent: Hash32,
        genesis: Hash32,
        address: Address,
        slot: Hash32,
    ) -> Package {
        let hash = h(number as u8 + 2);
        let code = vec![0x60, number as u8];
        let code_hash = Hash32(Keccak256::digest(&code).into());
        Package {
            mode: Mode::Delta,
            preceding_number: Some(number - 1),
            preceding_hash: Some(parent),
            input_sha256: h(number as u8),
            segment: Segment {
                schema: SEGMENT_SCHEMA.into(),
                chain_id: 1,
                genesis_hash: genesis,
                start_block: number,
                end_block: number,
                blocks: vec![BlockMeta {
                    number,
                    hash,
                    parent_hash: parent,
                    state_root: h(7),
                    timestamp: number,
                }],
                accounts: vec![AccountEvent {
                    block: number,
                    address,
                    exists: true,
                    incarnation: number,
                    nonce: number,
                    balance: [0; 32],
                    code_hash,
                }],
                storage: vec![StorageEvent {
                    block: number,
                    address,
                    incarnation: number,
                    slot,
                    value: [0; 32],
                }],
                code: vec![CodeBlob {
                    first_seen_block: number,
                    code_hash,
                    bytes: code,
                }],
            },
        }
    }
    fn anchor(address: Address, slot: Hash32) -> Result<Package> {
        let genesis = json!({"config":{"chainId":1}, "alloc":{(hex::encode(address.0)):
            {"balance":"0x1", "storage":{(slot.to_string()):format!("0x{}1", "0".repeat(63))}}}});
        let block = json!({"number":"0x0", "hash":h(1), "parentHash":h(0), "stateRoot":h(7), "timestamp":"0x0"});
        read_package(&genesis_anchor(
            &serde_json::to_vec(&genesis)?,
            &serde_json::to_vec(&block)?,
        )?)
    }
    async fn head(store: &dyn ArchiveStore) -> Result<Head> {
        Ok(read_head(store, 1).await?.unwrap().1)
    }
    async fn drain(store: &dyn ArchiveStore) -> Result<()> {
        while compact_once(store, 1).await? {}
        Ok(())
    }

    #[tokio::test]
    async fn publish_rejects_inconsistent_lifecycles() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let address = Address([0x11; 20]);
        let slot = h(2);
        let mut wrong_incarnation = anchor(address, slot)?;
        wrong_incarnation.segment.storage[0].incarnation = 1;
        let error = publish(&store, &wrong_incarnation, &gate(0, h(1)))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("incarnation"));
        let mut missing_code = anchor(address, slot)?;
        missing_code.segment.accounts[0].code_hash = h(9);
        let error = publish(&store, &missing_code, &gate(0, h(1)))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("code"));
        assert!(store.read_mutable(&head_key(1)).await?.is_none());
        publish(&store, &anchor(address, slot)?, &gate(0, h(1))).await?;
        let mut stale = delta(1, h(1), h(1), address, slot);
        stale.segment.storage[0].incarnation = 0;
        assert!(publish(&store, &stale, &gate(1, h(3))).await.is_err());
        assert_eq!(head(&store).await?.number, 0);
        Ok(())
    }

    #[tokio::test]
    async fn anchor_delta_recreation_zero_code_and_pinning() -> Result<()> {
        let address = Address([0x11; 20]);
        let slot = h(0x22);
        let anchor = anchor(address, slot)?;
        let store = MemoryArchiveStore::default();
        publish(&store, &anchor, &gate(0, h(1))).await?;
        let anchor_head = store.read_mutable(&head_key(1)).await?.unwrap();
        publish(&store, &anchor, &gate(0, h(1))).await?;
        assert_eq!(
            store.read_mutable(&head_key(1)).await?.unwrap().bytes,
            anchor_head.bytes
        );
        let pinned = Reader::open(&store, 1).await?;
        let one = delta(1, h(1), h(1), address, slot);
        publish(&store, &one, &gate(1, h(3))).await?;
        let delta_head = store.read_mutable(&head_key(1)).await?.unwrap();
        publish(&store, &one, &gate(1, h(3))).await?;
        assert_eq!(
            store.read_mutable(&head_key(1)).await?.unwrap().bytes,
            delta_head.bytes
        );
        assert_eq!(pinned.latest_block(), 0);
        assert!(pinned.account(address, 1).await.is_err());
        let reader = Reader::open(&store, 1).await?;
        assert_eq!(reader.storage(address, slot, 0).await?[31], 1);
        assert_eq!(reader.storage(address, slot, 1).await?, [0; 32]);
        assert_eq!(reader.account(address, 1).await?.unwrap().incarnation, 1);
        assert_eq!(reader.code(address, 1).await?, vec![0x60, 1]);
        assert_eq!(reader.account(Address([0x12; 20]), 1).await?, None);
        let mut deleted = delta(2, h(3), h(1), address, slot);
        deleted.segment.accounts[0].exists = false;
        deleted.segment.accounts[0].nonce = 0;
        deleted.segment.accounts[0].code_hash = Hash32([0; 32]);
        deleted.segment.storage.clear();
        deleted.segment.code.clear();
        publish(&store, &deleted, &gate(2, h(4))).await?;
        let recreated = delta(3, h(4), h(1), address, slot);
        publish(&store, &recreated, &gate(3, h(5))).await?;
        let reader = Reader::open(&store, 1).await?;
        assert!(!reader.account(address, 2).await?.unwrap().exists);
        assert_eq!(reader.storage(address, slot, 2).await?, [0; 32]);
        assert_eq!(reader.code(address, 2).await?, Vec::<u8>::new());
        assert_eq!(reader.account(address, 3).await?.unwrap().incarnation, 3);
        assert_eq!(reader.storage(address, slot, 3).await?, [0; 32]);
        assert_eq!(reader.storage(address, slot, 0).await?[31], 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_cas_never_replaces_the_winning_head() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let key = head_key(1);
        stage(&store, b"interrupted").await?;
        assert!(store.read_mutable(&key).await?.is_none());
        let stale = store.read_mutable(&key).await?;
        store.compare_and_swap(&key, None, b"winner").await?;
        assert!(store
            .compare_and_swap(&key, stale.as_ref(), b"loser")
            .await
            .is_err());
        assert_eq!(store.read_mutable(&key).await?.unwrap().bytes, b"winner");
        Ok(())
    }

    #[tokio::test]
    async fn backlog_blocks_publication_until_compaction_cascades_levels() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let address = Address([0x11; 20]);
        let slot = h(2);
        publish(&store, &anchor(address, slot)?, &gate(0, h(1))).await?;
        let mut parent = h(1);
        for n in 1..MAX_L0_BACKLOG as u64 {
            let package = delta(n, parent, h(1), address, slot);
            parent = package.segment.blocks[0].hash;
            publish(&store, &package, &gate(n, parent)).await?;
        }
        let n = MAX_L0_BACKLOG as u64;
        let blocked = delta(n, parent, h(1), address, slot);
        let hash = blocked.segment.blocks[0].hash;
        let error = publish(&store, &blocked, &gate(n, hash)).await.unwrap_err();
        assert!(error.to_string().contains("backlog"));
        drain(&store).await?;
        assert_eq!(head(&store).await?.l0.len(), 0);
        publish(&store, &blocked, &gate(n, hash)).await?;
        parent = hash;
        for n in n + 1..=170 {
            let package = delta(n, parent, h(1), address, slot);
            parent = package.segment.blocks[0].hash;
            publish(&store, &package, &gate(n, parent)).await?;
            drain(&store).await?;
        }
        // 171 epochs = one 160-weight L2 run, one 8-weight L1 run and 3 L0 runs.
        let head = head(&store).await?;
        let weights: Vec<_> = head
            .levels
            .iter()
            .map(|level| level.as_ref().map(|run| run.weight))
            .collect();
        assert_eq!(weights, vec![Some(8), Some(160)]);
        assert_eq!(head.l0.len(), 3);
        let reader = Reader::open(&store, 1).await?;
        assert_eq!(reader.run_count(), 5);
        for block in [0, 1, 31, 32, 159, 160, 167, 168, 170] {
            let account = reader.account(address, block).await?.unwrap();
            assert_eq!(account.incarnation, block);
            let expected = if block == 0 { 1 } else { 0 };
            assert_eq!(reader.storage(address, slot, block).await?[31], expected);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_compactor_never_loses_publications() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let store = &store;
        let address = Address([0x11; 20]);
        let slot = h(2);
        publish(store, &anchor(address, slot)?, &gate(0, h(1))).await?;
        let done = std::sync::atomic::AtomicBool::new(false);
        let done = &done;
        let compactor = async move {
            let mut commits = 0;
            while !done.load(Ordering::Relaxed) {
                if compact_once(store, 1).await? {
                    commits += 1;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            Ok::<_, anyhow::Error>(commits)
        };
        let publisher = async move {
            let mut parent = h(1);
            for n in 1..=80 {
                let package = delta(n, parent, h(1), address, slot);
                parent = package.segment.blocks[0].hash;
                loop {
                    match publish(store, &package, &gate(n, parent)).await {
                        Ok(()) => break,
                        Err(error) if error.to_string().contains("backlog") => {
                            tokio::task::yield_now().await
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            done.store(true, Ordering::Relaxed);
            Ok::<_, anyhow::Error>(())
        };
        let (commits, published) = tokio::join!(compactor, publisher);
        published?;
        assert!(commits? > 0);
        drain(store).await?;
        let reader = Reader::open(store, 1).await?;
        assert_eq!(reader.latest_block(), 80);
        for block in 0..=80 {
            assert_eq!(
                reader.account(address, block).await?.unwrap().incarnation,
                block
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn garbage_collection_keeps_every_read_and_frees_superseded_runs() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let address = Address([0x11; 20]);
        let slot = h(2);
        publish(&store, &anchor(address, slot)?, &gate(0, h(1))).await?;
        let mut parent = h(1);
        for n in 1..=40 {
            let package = delta(n, parent, h(1), address, slot);
            parent = package.segment.blocks[0].hash;
            publish(&store, &package, &gate(n, parent)).await?;
            drain(&store).await?;
        }
        let before = store.list("").await?.len();
        let zero = std::time::Duration::ZERO;
        let dry = collect_garbage(&store, 1, 2, zero, true).await?;
        assert!(dry.deleted_objects > 0);
        assert_eq!(store.list("").await?.len(), before);
        let report = collect_garbage(&store, 1, 2, zero, false).await?;
        assert_eq!(report.deleted_objects, dry.deleted_objects);
        assert_eq!(store.list("").await?.len(), before - report.deleted_objects);
        let reader = Reader::open(&store, 1).await?;
        for block in 0..=40 {
            assert_eq!(
                reader.account(address, block).await?.unwrap().incarnation,
                block
            );
            assert_eq!(
                reader.code(address, block).await?.len(),
                if block == 0 { 0 } else { 2 }
            );
            let expected = if block == 0 { 1 } else { 0 };
            assert_eq!(reader.storage(address, slot, block).await?[31], expected);
        }
        assert_eq!(
            collect_garbage(&store, 1, 2, zero, false)
                .await?
                .deleted_objects,
            0
        );
        // Writers keep working on the collected store.
        let package = delta(41, parent, h(1), address, slot);
        let hash = package.segment.blocks[0].hash;
        publish(&store, &package, &gate(41, hash)).await?;
        drain(&store).await?;
        assert_eq!(
            Reader::open(&store, 1)
                .await?
                .account(address, 41)
                .await?
                .unwrap()
                .nonce,
            41
        );
        Ok(())
    }

    #[tokio::test]
    async fn cold_account_plus_storage_gets_are_bounded_by_run_count() -> Result<()> {
        let store = crate::store::CountingStore::new(Arc::new(MemoryArchiveStore::default()));
        let cold = Address([0x11; 20]);
        let slot = h(2);
        publish(&store, &anchor(cold, slot)?, &gate(0, h(1))).await?;
        let hot = Address([0x33; 20]);
        let mut parent = h(1);
        for n in 1..=200 {
            let package = delta(n, parent, h(1), hot, slot);
            parent = package.segment.blocks[0].hash;
            publish(&store, &package, &gate(n, parent)).await?;
            drain(&store).await?;
        }
        let measure = |address: Address| {
            let store = &store;
            async move {
                store.take();
                let reader = Reader::open(store, 1).await?;
                let value = reader.storage(address, slot, 200).await?;
                Ok::<_, anyhow::Error>((value, store.take().0, reader.run_count() as u64))
            }
        };
        // Genesis-only state lives in the oldest run; every newer run is skipped by summary.
        let (value, gets, runs) = measure(cold).await?;
        assert_eq!(value[31], 1);
        assert!(gets <= 1 + runs + 6, "{gets} GETs over {runs} runs");
        let (value, gets, runs) = measure(Address([0x44; 20])).await?;
        assert_eq!(value, [0; 32]);
        assert!(gets <= 1 + runs, "{gets} GETs over {runs} runs");
        let (_, gets, _) = measure(hot).await?;
        assert!(gets <= 1 + 1 + 6, "{gets} GETs for newest-run state");
        assert!(runs <= 6, "{runs} runs");
        Ok(())
    }
}
