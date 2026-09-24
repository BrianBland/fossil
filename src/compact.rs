//! Bounded streaming compaction of adjacent immutable history runs.

use crate::format::Hash32;
use crate::run::{ObjectRef, Record, RunBuilder, RunReader, RunScanner};
use crate::store::ArchiveStore;
use crate::summary::SummaryBuilder;
use anyhow::{anyhow, bail, Result};
use futures_util::{stream, TryStreamExt};
use tokio::sync::mpsc;

const PAGE_LIMIT: usize = 1024 * 1024;
const IN_FLIGHT: usize = 32;

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

async fn next(scanner: &mut RunScanner, store: &dyn ArchiveStore) -> Result<Option<Record>> {
    scanner.next(|reference| fetch(store, reference)).await
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
    let mut scanners = Vec::with_capacity(runs.len());
    for (reference, bytes) in runs {
        scanners.push(RunReader::decode(*reference, bytes)?.scanner());
    }
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
            heads.push(next(scanner, store).await?);
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
            heads[index] = next(&mut scanners[index], store).await?;
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
            .await
    });

    let (produced, built, uploaded) = tokio::join!(producer, builder, uploads);
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
