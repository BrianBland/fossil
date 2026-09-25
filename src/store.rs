use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use futures_util::StreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjectPath;
use object_store::{GetResult, ObjectStore, PutMode, PutOptions, UpdateVersion};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tempfile::NamedTempFile;
use url::Url;

#[derive(Clone, Debug)]
pub struct VersionedBytes {
    pub bytes: Vec<u8>,
    pub version: String,
}

/// An identical object that already exists is re-written (same bytes) when it is
/// older than this, so garbage collection's age check sees it as freshly used.
pub const REFRESH_AFTER: std::time::Duration = std::time::Duration::from_secs(3600);

fn stale(modified: SystemTime) -> bool {
    modified.elapsed().is_ok_and(|age| age > REFRESH_AFTER)
}

/// One listed object, for inventory and garbage collection.
#[derive(Clone, Debug)]
pub struct Listed {
    pub key: String,
    pub size: u64,
    pub modified: SystemTime,
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    /// Every object whose key starts with `prefix` (keys relative to the store).
    async fn list(&self, prefix: &str) -> Result<Vec<Listed>>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Last-modified time of an object, or `None` when it does not exist.
    async fn modified(&self, key: &str) -> Result<Option<SystemTime>>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Fetch at most `maximum` bytes, rejecting provider metadata before buffering.
    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>>;
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()>;
    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>>;
    /// Read a mutable object without ever buffering more than `maximum` bytes.
    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()>;
}

pub async fn open_store(
    uri: &str,
    s3_endpoint: Option<&str>,
    s3_region: Option<&str>,
) -> Result<Arc<dyn ArchiveStore>> {
    if uri.starts_with("s3://") {
        let parsed = Url::parse(uri).context("invalid S3 store URI")?;
        let bucket = parsed
            .host_str()
            .ok_or_else(|| anyhow!("S3 URI must include a bucket"))?;
        let prefix = parsed.path().trim_matches('/').to_owned();
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            // object_store 0.11 otherwise rejects PutMode::Create/Update. Never
            // silently downgrade publication to last-writer-wins behavior.
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(endpoint) = s3_endpoint {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(region) = s3_region {
            builder = builder.with_region(region);
        }
        let inner = builder.build().context("create S3 object store")?;
        Ok(Arc::new(ObjectArchiveStore {
            inner: Arc::new(inner),
            prefix,
        }))
    } else if uri == "memory://" || uri == "memory:" {
        Ok(Arc::new(MemoryArchiveStore::default()))
    } else {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        let root = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            std::env::current_dir()?.join(path)
        };
        create_synced_directory_tree(&root).context("create filesystem store")?;
        Ok(Arc::new(FsArchiveStore { root }))
    }
}

#[derive(Debug, Default)]
pub struct MemoryArchiveStore {
    state: Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    entries: HashMap<String, MemoryEntry>,
    next_version: u64,
}

#[derive(Debug)]
struct MemoryEntry {
    bytes: Vec<u8>,
    version: u64,
}

impl MemoryState {
    fn insert(&mut self, key: &str, bytes: &[u8]) {
        self.next_version += 1;
        self.entries.insert(
            key.to_owned(),
            MemoryEntry {
                bytes: bytes.to_vec(),
                version: self.next_version,
            },
        );
    }
}

#[async_trait]
impl ArchiveStore for MemoryArchiveStore {
    async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        Ok(state
            .entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, entry)| Listed {
                key: key.clone(),
                size: entry.bytes.len() as u64,
                modified: SystemTime::UNIX_EPOCH,
            })
            .collect())
    }

    async fn modified(&self, key: &str) -> Result<Option<SystemTime>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        Ok(state
            .entries
            .contains_key(key)
            .then_some(SystemTime::UNIX_EPOCH))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?
            .entries
            .remove(key);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?
            .entries
            .get(key)
            .map(|entry| entry.bytes.clone())
            .ok_or_else(|| anyhow!("object not found: {key}"))
    }

    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        let entry = state
            .entries
            .get(key)
            .ok_or_else(|| anyhow!("object not found: {key}"))?;
        if entry.bytes.len() > maximum {
            bail!("object metadata length exceeds bounded GET limit");
        }
        Ok(entry.bytes.clone())
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        match state.entries.get(key) {
            Some(entry) if entry.bytes == bytes => Ok(()),
            Some(_) => bail!("immutable object collision at {key}"),
            None => {
                state.insert(key, bytes);
                Ok(())
            }
        }
    }

    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?
            .entries
            .get(key)
            .map(|entry| VersionedBytes {
                bytes: entry.bytes.clone(),
                version: entry.version.to_string(),
            }))
    }

    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        let Some(entry) = state.entries.get(key) else {
            return Ok(None);
        };
        if entry.bytes.len() > maximum {
            bail!("mutable object exceeds bounded read limit");
        }
        Ok(Some(VersionedBytes {
            bytes: entry.bytes.clone(),
            version: entry.version.to_string(),
        }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("memory store lock poisoned"))?;
        let matches = match (expected, state.entries.get(key)) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected.version == current.version.to_string(),
            _ => false,
        };
        if !matches {
            bail!("conditional head update failed");
        }
        state.insert(key, bytes);
        Ok(())
    }
}

/// Wraps a store and counts every read request (immutable GETs and head reads).
/// Used to measure cold-read cost; writes pass through uncounted.
pub struct CountingStore {
    pub inner: Arc<dyn ArchiveStore>,
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ArchiveStore>) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    /// Return and reset `(gets, bytes)`.
    pub fn take(&self) -> (u64, u64) {
        (
            self.gets.swap(0, Ordering::Relaxed),
            self.bytes.swap(0, Ordering::Relaxed),
        )
    }

    fn count(&self, bytes: usize) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[async_trait]
impl ArchiveStore for CountingStore {
    async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
    async fn modified(&self, key: &str) -> Result<Option<SystemTime>> {
        self.inner.modified(key).await
    }
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let bytes = self.inner.get(key).await?;
        self.count(bytes.len());
        Ok(bytes)
    }
    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let result = self.inner.get_bounded(key, maximum).await;
        self.count(result.as_ref().map_or(0, Vec::len));
        result
    }
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes).await
    }
    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        let result = self.inner.read_mutable(key).await;
        self.count(0);
        result
    }
    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        let result = self.inner.read_mutable_bounded(key, maximum).await;
        self.count(0);
        result
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

#[derive(Debug)]
pub struct FsArchiveStore {
    root: PathBuf,
}

impl FsArchiveStore {
    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn sync_path_directories(&self, path: &Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| anyhow!("path has no parent"))?;
        let relative = parent
            .strip_prefix(&self.root)
            .context("store path escaped root")?;
        let mut directory = self.root.clone();
        sync_directory(&directory)?;
        for component in relative.components() {
            directory.push(component);
            sync_directory(&directory)?;
        }
        Ok(())
    }

    fn create_parent_directories(&self, path: &Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| anyhow!("path has no parent"))?;
        std::fs::create_dir_all(parent)?;
        // Sync every directory from the store root through the leaf. This persists
        // newly-created ancestor entries before a later head rename can expose them.
        self.sync_path_directories(path)
    }

    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        self.create_parent_directories(path)?;
        let parent = path.parent().ok_or_else(|| anyhow!("path has no parent"))?;
        let mut temporary = NamedTempFile::new_in(parent)?;
        std::io::Write::write_all(&mut temporary, bytes)?;
        temporary.as_file().sync_all()?;
        temporary.persist(path).map_err(|error| error.error)?;
        self.sync_path_directories(path)?;
        Ok(())
    }
}

fn create_synced_directory_tree(path: &Path) -> Result<()> {
    if path.exists() {
        return sync_directory(path);
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("filesystem store path has no parent"))?;
    create_synced_directory_tree(parent)?;
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    // Persist the new directory entry before any later head update can make
    // archive contents visible.
    sync_directory(parent)?;
    sync_directory(path)
}

fn read_file_bounded(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let limit = maximum
        .checked_add(1)
        .context("bounded read limit overflow")?;
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1024));
    file.by_ref().take(limit as u64).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        bail!("object stream exceeds bounded read limit");
    }
    Ok(Some(bytes))
}

fn sync_directory(path: &Path) -> Result<()> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("open directory {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync directory {}", path.display()))
}

#[async_trait]
impl ArchiveStore for FsArchiveStore {
    async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        let mut out = Vec::new();
        let mut pending = vec![self.root.clone()];
        while let Some(directory) = pending.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                let metadata = entry.metadata()?;
                let path = entry.path();
                if metadata.is_dir() {
                    pending.push(path);
                    continue;
                }
                // Skip in-flight temporary files of a concurrent writer.
                if entry.file_name().to_string_lossy().starts_with(".tmp") {
                    continue;
                }
                let key = path
                    .strip_prefix(&self.root)?
                    .to_string_lossy()
                    .replace('\\', "/");
                if key.starts_with(prefix) {
                    out.push(Listed {
                        key,
                        size: metadata.len(),
                        modified: metadata.modified()?,
                    });
                }
            }
        }
        Ok(out)
    }

    async fn modified(&self, key: &str) -> Result<Option<SystemTime>> {
        match std::fs::metadata(self.path(key)) {
            Ok(metadata) => Ok(Some(metadata.modified()?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match std::fs::remove_file(self.path(key)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        std::fs::read(self.path(key)).with_context(|| format!("read object {key}"))
    }

    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        read_file_bounded(&self.path(key), maximum)?
            .ok_or_else(|| anyhow!("object not found: {key}"))
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path(key);
        if path.exists() {
            let existing = self.get_bounded(key, bytes.len()).await?;
            if existing != bytes {
                bail!("immutable object collision at {key}");
            }
            if stale(std::fs::metadata(&path)?.modified()?) {
                File::options()
                    .write(true)
                    .open(&path)?
                    .set_modified(SystemTime::now())?;
            }
            return Ok(());
        }
        self.create_parent_directories(&path)?;
        let parent = path.parent().ok_or_else(|| anyhow!("path has no parent"))?;
        let mut temporary = NamedTempFile::new_in(parent)?;
        std::io::Write::write_all(&mut temporary, bytes)?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => {
                self.sync_path_directories(&path)?;
                Ok(())
            }
            Err(error) => {
                if std::fs::read(&path).ok().as_deref() == Some(bytes) {
                    // The winning writer is responsible for syncing the same entry;
                    // syncing again is cheap and closes the race for this process.
                    self.sync_path_directories(&path)?;
                    Ok(())
                } else {
                    Err(error.error).context("create immutable object")
                }
            }
        }
    }

    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        let path = self.path(key);
        match std::fs::read(path) {
            Ok(bytes) => {
                let version = crate::format::Hash32::digest(&bytes).to_string();
                Ok(Some(VersionedBytes { bytes, version }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        Ok(
            read_file_bounded(&self.path(key), maximum)?.map(|bytes| VersionedBytes {
                version: crate::format::Hash32::digest(&bytes).to_string(),
                bytes,
            }),
        )
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()> {
        let lock_path = self.root.join(".fossil-publish.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        sync_directory(&self.root)?;
        lock.lock_exclusive()?;
        let maximum = expected.map_or(bytes.len(), |value| value.bytes.len().max(bytes.len()));
        let current = self.read_mutable_bounded(key, maximum).await?;
        let matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected.version == current.version,
            _ => false,
        };
        if !matches {
            FileExt::unlock(&lock)?;
            bail!("conditional head update failed");
        }
        self.atomic_write(&self.path(key), bytes)?;
        FileExt::unlock(&lock)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ObjectArchiveStore {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectArchiveStore {
    fn key(&self, key: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else {
            ObjectPath::from(format!("{}/{key}", self.prefix))
        }
    }
}

async fn collect_object_stream_bounded(result: GetResult, maximum: usize) -> Result<Vec<u8>> {
    let advertised = result.meta.size;
    if advertised > maximum {
        bail!("object metadata length exceeds bounded GET limit");
    }
    let mut bytes = Vec::with_capacity(advertised.min(maximum));
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .context("object stream length overflow")?;
        if next > maximum {
            bail!("object stream exceeds bounded GET limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != advertised {
        bail!("provider metadata/response length mismatch");
    }
    Ok(bytes)
}

#[async_trait]
impl ArchiveStore for ObjectArchiveStore {
    async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        let root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let mut stream = self.inner.list(Some(&self.key(prefix)));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            let key = meta.location.to_string();
            let key = key.strip_prefix(&root).unwrap_or(&key).to_owned();
            if key.starts_with(prefix) {
                out.push(Listed {
                    key,
                    size: meta.size as u64,
                    modified: meta.last_modified.into(),
                });
            }
        }
        Ok(out)
    }

    async fn modified(&self, key: &str) -> Result<Option<SystemTime>> {
        match self.inner.head(&self.key(key)).await {
            Ok(meta) => Ok(Some(meta.last_modified.into())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self.inner.delete(&self.key(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let result = self.inner.get(&self.key(key)).await?;
        Ok(result.bytes().await?.to_vec())
    }

    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let result = self.inner.get(&self.key(key)).await?;
        collect_object_stream_bounded(result, maximum).await
    }

    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.key(key);
        let result = self
            .inner
            .put_opts(
                &path,
                Bytes::copy_from_slice(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let found = self.inner.get(&path).await?;
                let modified: SystemTime = found.meta.last_modified.into();
                let existing = collect_object_stream_bounded(found, bytes.len()).await?;
                if existing != bytes {
                    bail!("immutable object collision at {key}")
                }
                // Same bytes, so overwriting is invisible to readers; it only
                // resets the age garbage collection uses to spare reused objects.
                if stale(modified) {
                    self.inner
                        .put_opts(
                            &path,
                            Bytes::copy_from_slice(bytes).into(),
                            PutOptions::default(),
                        )
                        .await?;
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        match self.inner.get(&self.key(key)).await {
            Ok(result) => {
                let version = result
                    .meta
                    .e_tag
                    .clone()
                    .ok_or_else(|| anyhow!("provider returned no ETag for mutable object"))?;
                let bytes = result.bytes().await?.to_vec();
                Ok(Some(VersionedBytes { bytes, version }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        match self.inner.get(&self.key(key)).await {
            Ok(result) => {
                let version = result
                    .meta
                    .e_tag
                    .clone()
                    .ok_or_else(|| anyhow!("provider returned no ETag for mutable object"))?;
                let bytes = collect_object_stream_bounded(result, maximum).await?;
                Ok(Some(VersionedBytes { bytes, version }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()> {
        let mode = match expected {
            None => PutMode::Create,
            Some(expected) => PutMode::Update(UpdateVersion {
                e_tag: Some(expected.version.clone()),
                version: None,
            }),
        };
        self.inner
            .put_opts(
                &self.key(key),
                Bytes::copy_from_slice(bytes).into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
            .context("conditional head update failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn bounded_get_rejects_oversized_metadata_before_returning_bytes() {
        let memory = MemoryArchiveStore::default();
        memory.put_immutable("large", &[7; 16]).await.unwrap();
        assert!(memory.get_bounded("large", 8).await.is_err());
        memory
            .compare_and_swap("head", None, &[7; 16])
            .await
            .unwrap();
        assert!(memory.read_mutable_bounded("head", 8).await.is_err());

        let root = tempfile::tempdir().unwrap();
        let filesystem = FsArchiveStore {
            root: root.path().to_path_buf(),
        };
        filesystem.put_immutable("large", &[7; 16]).await.unwrap();
        assert!(filesystem.get_bounded("large", 8).await.is_err());
        filesystem
            .compare_and_swap("head", None, &[7; 16])
            .await
            .unwrap();
        assert!(filesystem.read_mutable_bounded("head", 8).await.is_err());

        let object = ObjectArchiveStore {
            inner: Arc::new(InMemory::new()),
            prefix: "bounded".to_owned(),
        };
        object.put_immutable("large", &[7; 16]).await.unwrap();
        assert!(object.get_bounded("large", 8).await.is_err());
        object
            .compare_and_swap("head", None, &[7; 16])
            .await
            .unwrap();
        assert!(object.read_mutable_bounded("head", 8).await.is_err());

        let inner = InMemory::new();
        let path = ObjectPath::from("underreported");
        inner
            .put(&path, Bytes::from_static(&[9; 16]).into())
            .await
            .unwrap();
        let mut result = inner.get(&path).await.unwrap();
        result.meta.size = 4;
        let error = collect_object_stream_bounded(result, 8).await.unwrap_err();
        assert!(error.to_string().contains("stream exceeds"));
    }

    #[tokio::test]
    async fn object_adapter_uses_conditional_modes() {
        let store = ObjectArchiveStore {
            inner: Arc::new(InMemory::new()),
            prefix: "test".to_owned(),
        };
        store.put_immutable("object", b"one").await.unwrap();
        store.put_immutable("object", b"one").await.unwrap();
        assert!(store.put_immutable("object", b"two").await.is_err());

        store.compare_and_swap("head", None, b"one").await.unwrap();
        let first = store.read_mutable("head").await.unwrap().unwrap();
        store
            .compare_and_swap("head", Some(&first), b"two")
            .await
            .unwrap();
        assert!(store
            .compare_and_swap("head", Some(&first), b"stale")
            .await
            .is_err());
    }
}
