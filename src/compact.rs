//! Bounded streaming compaction of adjacent immutable history runs.

use crate::format::Hash32;
use crate::run::{ObjectRef, Record, RunBuilder, RunReader, RunScanner};
use crate::store::ArchiveStore;
use crate::summary::SummaryBuilder;
use anyhow::{anyhow, bail, Result};
use futures_util::{stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::mpsc;

const PAGE_LIMIT: usize = 1024 * 1024;
const IN_FLIGHT: usize = 32;
/// Total data pages a merge may hold ahead of its scanners.
const READ_AHEAD_PAGES: usize = 768;
const FETCH_CONCURRENCY: usize = 64;
const PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const STALL_LIMIT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

#[derive(Default)]
struct Progress {
    records: AtomicU64,
    uploaded: AtomicU64,
}

/// A written run: every data, index, root and summary object is durable.
#[derive(Clone, Debug)]
pub struct BuiltRun {
    pub root: ObjectRef,
    pub root_bytes: Vec<u8>,
    pub keys: u64,
    pub shards: u32,
}

pub async fn fetch(store: &dyn ArchiveStore, reference: ObjectRef) -> Result<Vec<u8>> {
    let limit = reference.length as usize;
    if limit == 0 || limit > PAGE_LIMIT || reference.digest == Hash32([0; 32]) {
        bail!("invalid run object reference");
    }
    let bytes = store
        .get_bounded(&reference.digest.object_key(), limit)
        .await?;
    if bytes.len() != limit || Hash32::digest(&bytes) != reference.digest {
        bail!("run object digest or length mismatch");
    }
    Ok(bytes)
}

#[cfg(test)]
async fn next(scanner: &mut RunScanner, store: &dyn ArchiveStore) -> Result<Option<Record>> {
    scanner.next(|reference| fetch(store, reference)).await
}

/// Data pages fetched ahead of every partition scanner. A merge reads all of
/// each run's partitions in order and at similar rates (keys are hash-routed),
/// so on any miss the reader tops up every partition that is running low in one
/// concurrent batch. Without this a merge waits on one R2 round trip per page.
// ponytail: at most READ_AHEAD_PAGES pages are buffered in total (about 256 MiB
// of encoded pages worst case); make it configurable if compactor RAM matters.
struct ReadAhead<'a> {
    store: &'a dyn ArchiveStore,
    /// Pages fetched so far (progress reporting).
    pages: AtomicU64,
    /// Page digest -> (list, position) in `lists`.
    order: HashMap<Hash32, (usize, usize)>,
    lists: Vec<Vec<ObjectRef>>,
    /// Pages kept ahead of the last request, per list.
    depth: usize,
    state: Mutex<AheadState>,
}

struct AheadState {
    ready: HashMap<Hash32, Vec<u8>>,
    /// Next page per list that has not been fetched.
    fetched: Vec<usize>,
    /// Position of the latest request per list.
    wanted: Vec<usize>,
}

impl<'a> ReadAhead<'a> {
    async fn new(store: &'a dyn ArchiveStore, readers: &[RunReader]) -> Result<Self> {
        let mut lists = Vec::new();
        for reader in readers {
            lists.extend(
                reader
                    .data_pages(|reference| fetch(store, reference))
                    .await?,
            );
        }
        let mut order = HashMap::new();
        for (list, pages) in lists.iter().enumerate() {
            for (position, page) in pages.iter().enumerate() {
                order.entry(page.digest).or_insert((list, position));
            }
        }
        let busy = lists
            .iter()
            .filter(|pages| !pages.is_empty())
            .count()
            .max(1);
        Ok(Self {
            store,
            pages: AtomicU64::new(0),
            order,
            depth: (READ_AHEAD_PAGES / busy).clamp(2, 32),
            state: Mutex::new(AheadState {
                ready: HashMap::new(),
                fetched: vec![0; lists.len()],
                wanted: vec![0; lists.len()],
            }),
            lists,
        })
    }

    async fn get(&self, reference: ObjectRef) -> Result<Vec<u8>> {
        let planned = {
            let mut state = self.state.lock().unwrap();
            if let Some(bytes) = state.ready.remove(&reference.digest) {
                if let Some(&(list, position)) = self.order.get(&reference.digest) {
                    state.wanted[list] = state.wanted[list].max(position);
                }
                return Ok(bytes);
            }
            self.order.get(&reference.digest).map(|&(list, position)| {
                state.wanted[list] = state.wanted[list].max(position);
                state.fetched[list] = state.fetched[list].max(position);
                let mut batch = vec![reference];
                for (index, pages) in self.lists.iter().enumerate() {
                    let start = state.fetched[index];
                    let end = pages.len().min(state.wanted[index] + 1 + self.depth);
                    // Only top up lists that have used at least half of their lead.
                    if start < end && (index == list || end - start > self.depth / 2) {
                        batch.extend(
                            pages[start..end]
                                .iter()
                                .filter(|page| page.digest != reference.digest),
                        );
                        state.fetched[index] = end;
                    }
                }
                batch
            })
        };
        // Not a data page (for example a fence object): fetch it directly.
        let Some(batch) = planned else {
            return fetch(self.store, reference).await;
        };
        let mut fetched: Vec<(ObjectRef, Vec<u8>)> =
            futures_util::stream::iter(batch.into_iter().map(|page| async move {
                Ok::<_, anyhow::Error>((page, fetch(self.store, page).await?))
            }))
            .buffer_unordered(FETCH_CONCURRENCY)
            .try_collect()
            .await?;
        self.pages
            .fetch_add(fetched.len() as u64, Ordering::Relaxed);
        let position = fetched
            .iter()
            .position(|(page, _)| page.digest == reference.digest)
            .expect("batch contains the request");
        let (_, first) = fetched.swap_remove(position);
        self.state.lock().unwrap().ready.extend(
            fetched
                .into_iter()
                .map(|(page, bytes)| (page.digest, bytes)),
        );
        Ok(first)
    }
}

async fn put_all(store: &dyn ArchiveStore, objects: Vec<(String, Vec<u8>)>) -> Result<()> {
    stream::iter(objects.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(IN_FLIGHT, |(key, bytes)| async move {
            store.put_immutable(&key, &bytes).await
        })
        .await
}

/// Build a run from records already sorted by `(key, block)` and held in memory.
pub async fn write_sorted(store: &dyn ArchiveStore, records: Vec<Record>) -> Result<BuiltRun> {
    let mut distinct = 0_u64;
    for (index, record) in records.iter().enumerate() {
        if index == 0 || records[index - 1].key != record.key {
            distinct += 1;
        }
    }
    let mut summary = SummaryBuilder::new(distinct)?;
    for (index, record) in records.iter().enumerate() {
        if index == 0 || records[index - 1].key != record.key {
            summary.add(&record.key);
        }
    }
    let mut objects = Vec::new();
    let root = RunBuilder::build(records, |object| {
        objects.push((object.object_key(), object.bytes));
        Ok(())
    })?;
    let root_bytes = objects.last().expect("root is emitted last").1.clone();
    put_all(store, objects).await?;
    let (keys, shards) = (summary.keys(), summary.shards());
    put_all(store, summary.finish(root.digest)).await?;
    Ok(BuiltRun {
        root,
        root_bytes,
        keys,
        shards,
    })
}

/// K-way merge of adjacent time-disjoint runs, oldest first, without collecting
/// any run in memory. Duplicate `(key, block)` versions are rejected. The caller
/// verifies the block intervals; the returned run may then be committed.
pub async fn merge_runs(
    store: &dyn ArchiveStore,
    runs: &[(ObjectRef, Vec<u8>)],
    expected_keys: u64,
) -> Result<BuiltRun> {
    let mut readers = Vec::with_capacity(runs.len());
    for (reference, bytes) in runs {
        readers.push(RunReader::decode(*reference, bytes)?);
    }
    let read_ahead = ReadAhead::new(store, &readers).await?;
    let read_ahead = &read_ahead;
    let progress = Progress::default();
    let progress = &progress;
    let built_count = std::sync::Arc::new(AtomicU64::new(0));
    let built_counter = built_count.clone();
    let mut scanners: Vec<RunScanner> = readers.iter().map(RunReader::scanner).collect();
    let (records_tx, mut records_rx) = mpsc::channel::<Record>(IN_FLIGHT);
    let (objects_tx, objects_rx) = mpsc::channel(IN_FLIGHT);

    let builder = tokio::task::spawn_blocking(move || {
        let records = std::iter::from_fn(|| records_rx.blocking_recv());
        let mut root_bytes = Vec::new();
        let root = RunBuilder::build(records, |object| {
            // The small root is emitted last, after every page and index.
            if object.bytes.len() < 4096 {
                root_bytes.clone_from(&object.bytes);
            }
            built_counter.fetch_add(1, Ordering::Relaxed);
            objects_tx
                .blocking_send(object)
                .map_err(|_| anyhow!("compaction object uploader stopped"))
        })?;
        Ok::<_, anyhow::Error>((root, root_bytes))
    });

    let producer = async move {
        let mut summary = SummaryBuilder::new(expected_keys)?;
        let mut heads = Vec::with_capacity(scanners.len());
        for scanner in &mut scanners {
            heads.push(scanner.next(|page| read_ahead.get(page)).await?);
        }
        let mut last_key: Option<Vec<u8>> = None;
        loop {
            let mut chosen: Option<usize> = None;
            for (index, head) in heads.iter().enumerate() {
                let Some(record) = head else { continue };
                if let Some(best) = chosen {
                    let current = heads[best].as_ref().unwrap();
                    let ordering = (&record.key, record.block).cmp(&(&current.key, current.block));
                    if ordering.is_eq() {
                        bail!("duplicate (key, block) across merged runs");
                    }
                    if ordering.is_lt() {
                        chosen = Some(index);
                    }
                } else {
                    chosen = Some(index);
                }
            }
            let Some(index) = chosen else { break };
            let record = heads[index].take().unwrap();
            heads[index] = scanners[index].next(|page| read_ahead.get(page)).await?;
            progress.records.fetch_add(1, Ordering::Relaxed);
            if last_key.as_deref() != Some(record.key.as_slice()) {
                summary.add(&record.key);
                last_key = Some(record.key.clone());
            }
            records_tx
                .send(record)
                .await
                .map_err(|_| anyhow!("compaction record builder stopped"))?;
        }
        Ok::<_, anyhow::Error>(summary)
    };

    let uploads = stream::unfold(objects_rx, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|object| (Ok::<_, anyhow::Error>(object), receiver))
    })
    .try_for_each_concurrent(IN_FLIGHT, |object| async move {
        store
            .put_immutable(&object.object_key(), &object.bytes)
            .await?;
        progress.uploaded.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });

    // Log progress, and abandon a merge that makes none for STALL_LIMIT (the
    // compactor then exits and its supervisor restarts it).
    let watchdog = async {
        let mut last = (u64::MAX, 0, 0, 0);
        let mut idle = std::time::Duration::ZERO;
        loop {
            tokio::time::sleep(PROGRESS_INTERVAL).await;
            let now = (
                read_ahead.pages.load(Ordering::Relaxed),
                progress.records.load(Ordering::Relaxed),
                built_count.load(Ordering::Relaxed),
                progress.uploaded.load(Ordering::Relaxed),
            );
            tracing::info!(
                pages = now.0,
                records = now.1,
                objects_built = now.2,
                objects_uploaded = now.3,
                "merge progress"
            );
            idle = if now == last {
                idle + PROGRESS_INTERVAL
            } else {
                Default::default()
            };
            if idle >= STALL_LIMIT {
                return anyhow!(
                    "merge stalled for {idle:?} (pages={}, records={}, built={}, uploaded={})",
                    now.0,
                    now.1,
                    now.2,
                    now.3
                );
            }
            last = now;
        }
    };
    let work = async { tokio::join!(producer, builder, uploads) };
    let (produced, built, uploaded) = tokio::select! {
        done = work => done,
        stalled = watchdog => return Err(stalled),
    };
    let summary = produced?;
    let (root, root_bytes) = built??;
    uploaded?;
    let (keys, shards) = (summary.keys(), summary.shards());
    put_all(store, summary.finish(root.digest)).await?;
    Ok(BuiltRun {
        root,
        root_bytes,
        keys,
        shards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryArchiveStore;

    fn record(key: &[u8], block: u64, value: &[u8]) -> Record {
        Record {
            key: key.to_vec(),
            block,
            value: value.to_vec(),
        }
    }
    fn input(run: &BuiltRun) -> (ObjectRef, Vec<u8>) {
        (run.root, run.root_bytes.clone())
    }

    #[tokio::test]
    async fn merged_run_preserves_history_tombstones_and_global_order() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let a = write_sorted(
            &store,
            vec![record(b"a", 0, b"old"), record(b"z", 0, &[0; 32])],
        )
        .await?;
        let b = write_sorted(&store, vec![record(b"a", 1, b""), record(b"b", 1, b"new")]).await?;
        let c = write_sorted(&store, vec![record(b"b", 2, b"newer")]).await?;
        let merged = merge_runs(&store, &[input(&a), input(&b), input(&c)], 6).await?;
        assert_eq!(merged.keys, 3);
        let reader = RunReader::decode(merged.root, &merged.root_bytes)?;
        let get = |key: &'static [u8], block| reader.lookup(key, block, |r| fetch(&store, r));
        assert_eq!(get(b"a", 0).await?, Some(b"old".to_vec()));
        assert_eq!(get(b"a", 1).await?, Some(Vec::new()));
        assert_eq!(get(b"b", 1).await?, Some(b"new".to_vec()));
        assert_eq!(get(b"b", 9).await?, Some(b"newer".to_vec()));
        assert_eq!(get(b"z", 2).await?, Some(vec![0; 32]));
        let mut scanner = reader.scanner();
        let mut output = Vec::new();
        while let Some(next) = next(&mut scanner, &store).await? {
            output.push((next.key, next.block));
        }
        assert_eq!(
            output,
            vec![
                (b"a".to_vec(), 0),
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 1),
                (b"b".to_vec(), 2),
                (b"z".to_vec(), 0)
            ]
        );
        Ok(())
    }

    /// Delays every GET and records the peak number in flight.
    struct Slow {
        inner: MemoryArchiveStore,
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ArchiveStore for Slow {
        async fn list(&self, prefix: &str) -> Result<Vec<crate::store::Listed>> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
        async fn modified(&self, key: &str) -> Result<Option<std::time::SystemTime>> {
            self.inner.modified(key).await
        }
        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key).await
        }
        async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            self.in_flight.fetch_sub(1, SeqCst);
            self.inner.get_bounded(key, maximum).await
        }
        async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
            self.inner.put_immutable(key, bytes).await
        }
        async fn read_mutable(&self, key: &str) -> Result<Option<crate::store::VersionedBytes>> {
            self.inner.read_mutable(key).await
        }
        async fn read_mutable_bounded(
            &self,
            key: &str,
            maximum: usize,
        ) -> Result<Option<crate::store::VersionedBytes>> {
            self.inner.read_mutable_bounded(key, maximum).await
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            expected: Option<&crate::store::VersionedBytes>,
            bytes: &[u8],
        ) -> Result<()> {
            self.inner.compare_and_swap(key, expected, bytes).await
        }
    }

    #[tokio::test]
    async fn merge_reads_pages_ahead_and_keeps_every_record() -> Result<()> {
        let store = Slow {
            inner: MemoryArchiveStore::default(),
            in_flight: 0.into(),
            peak: 0.into(),
        };
        let run = |block: u64| {
            let mut records: Vec<_> = (0..20_000_u32)
                .map(|key| record(&key.to_be_bytes(), block, &[block as u8; 2000]))
                .collect();
            records.sort_by(|a, b| a.key.cmp(&b.key));
            records
        };
        let a = write_sorted(&store, run(1)).await?;
        let b = write_sorted(&store, run(2)).await?;
        let merged = merge_runs(&store, &[input(&a), input(&b)], 40_000).await?;
        assert_eq!(merged.keys, 20_000);
        assert!(store.peak.load(std::sync::atomic::Ordering::SeqCst) > 1);
        let reader = RunReader::decode(merged.root, &merged.root_bytes)?;
        let mut scanner = reader.scanner();
        let mut count = 0;
        while next(&mut scanner, &store).await?.is_some() {
            count += 1;
        }
        assert_eq!(count, 40_000);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_block_version_cannot_create_a_merged_root() -> Result<()> {
        let store = MemoryArchiveStore::default();
        let a = write_sorted(&store, vec![record(b"k", 1, b"old")]).await?;
        let b = write_sorted(&store, vec![record(b"k", 1, b"conflict")]).await?;
        assert!(merge_runs(&store, &[input(&a), input(&b)], 2)
            .await
            .is_err());
        Ok(())
    }
}
