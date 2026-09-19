use crate::archive::{decode_index, decode_segment};
use crate::format::{Hash32, Segment, SegmentDescriptor, SegmentIndex};
use crate::store::ArchiveStore;
use anyhow::{Context, Result};
use moka::sync::Cache;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct CacheStats {
    pub object_gets: AtomicU64,
    pub bytes_read: AtomicU64,
    pub memory_hits: AtomicU64,
    pub disk_hits: AtomicU64,
    pub misses: AtomicU64,
    pub corruptions: AtomicU64,
}

impl CacheStats {
    pub fn render(&self) -> String {
        format!(
            concat!(
                "fossil_object_gets_total {}\n",
                "fossil_object_bytes_total {}\n",
                "fossil_cache_memory_hits_total {}\n",
                "fossil_cache_disk_hits_total {}\n",
                "fossil_cache_misses_total {}\n",
                "fossil_cache_corruptions_total {}\n"
            ),
            self.object_gets.load(Ordering::Relaxed),
            self.bytes_read.load(Ordering::Relaxed),
            self.memory_hits.load(Ordering::Relaxed),
            self.disk_hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.corruptions.load(Ordering::Relaxed),
        )
    }
}

pub struct SegmentCache {
    store: Arc<dyn ArchiveStore>,
    root: PathBuf,
    memory: Cache<Hash32, Arc<Segment>>,
    miss_lock: Mutex<()>,
    pub stats: Arc<CacheStats>,
}

impl SegmentCache {
    pub fn new(store: Arc<dyn ArchiveStore>, root: PathBuf, memory_mib: u64) -> Result<Self> {
        std::fs::create_dir_all(root.join("indexes"))?;
        std::fs::create_dir_all(root.join("segments"))?;
        let memory = Cache::builder()
            .max_capacity(memory_mib.saturating_mul(1024 * 1024))
            .weigher(|_key: &Hash32, value: &Arc<Segment>| -> u32 {
                value
                    .accounts
                    .len()
                    .saturating_mul(96)
                    .saturating_add(value.storage.len().saturating_mul(96))
                    .saturating_add(value.code.iter().map(|blob| blob.bytes.len()).sum())
                    .clamp(1, u32::MAX as usize) as u32
            })
            .build();
        Ok(Self {
            store,
            root,
            memory,
            miss_lock: Mutex::new(()),
            stats: Arc::new(CacheStats::default()),
        })
    }

    pub async fn mirror_index(&self, descriptor: &SegmentDescriptor) -> Result<SegmentIndex> {
        let path = self
            .root
            .join("indexes")
            .join(hex::encode(descriptor.index_sha256.0));
        if let Ok(bytes) = std::fs::read(&path) {
            match decode_index(descriptor, &bytes) {
                Ok(index) => return Ok(index),
                Err(_) => {
                    self.stats.corruptions.fetch_add(1, Ordering::Relaxed);
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        let bytes = self.fetch(&descriptor.index_sha256.object_key()).await?;
        let index = decode_index(descriptor, &bytes)?;
        atomic_cache_write(&path, &bytes)?;
        Ok(index)
    }

    pub async fn segment(&self, descriptor: &SegmentDescriptor) -> Result<Arc<Segment>> {
        if let Some(segment) = self.memory.get(&descriptor.segment_sha256) {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(segment);
        }
        let _guard = self.miss_lock.lock().await;
        if let Some(segment) = self.memory.get(&descriptor.segment_sha256) {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(segment);
        }
        let path = self
            .root
            .join("segments")
            .join(hex::encode(descriptor.segment_sha256.0));
        if let Ok(bytes) = std::fs::read(&path) {
            match decode_segment(descriptor, &bytes) {
                Ok(segment) => {
                    self.stats.disk_hits.fetch_add(1, Ordering::Relaxed);
                    let segment = Arc::new(segment);
                    self.memory
                        .insert(descriptor.segment_sha256, segment.clone());
                    return Ok(segment);
                }
                Err(_) => {
                    self.stats.corruptions.fetch_add(1, Ordering::Relaxed);
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        let bytes = self.fetch(&descriptor.segment_sha256.object_key()).await?;
        let segment = Arc::new(decode_segment(descriptor, &bytes)?);
        atomic_cache_write(&path, &bytes)?;
        self.memory
            .insert(descriptor.segment_sha256, segment.clone());
        Ok(segment)
    }

    async fn fetch(&self, key: &str) -> Result<Vec<u8>> {
        let bytes = self.store.get(key).await?;
        self.stats.object_gets.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

fn atomic_cache_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("cache path has no parent")?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    std::io::Write::write_all(&mut temporary, bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
