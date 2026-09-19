use crate::format::{
    canonical_json, quantity, Hash32, Head, IndexEntry, Manifest, Segment, SegmentDescriptor,
    SegmentIndex, INDEX_SCHEMA, MANIFEST_SCHEMA, SEGMENT_SCHEMA,
};
use crate::normalized::{Mode, Package};
use crate::store::{ArchiveStore, VersionedBytes};
use anyhow::{anyhow, bail, Context, Result};
use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
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

impl PublicationGate {
    fn eligible(&self) -> u64 {
        match self {
            Self::Finalized { number, .. } => *number,
            Self::FixedOffset {
                observed_number,
                offset,
                ..
            } => observed_number.saturating_sub(*offset),
        }
    }

    fn description(&self) -> String {
        match self {
            Self::Finalized { number, hash } => {
                format!("finalized:{}:{hash}", quantity(*number))
            }
            Self::FixedOffset {
                observed_number,
                observed_hash,
                offset,
            } => format!(
                "fixed-offset:{}:{observed_hash}:{offset}",
                quantity(*observed_number)
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArchiveOutcome {
    pub manifest: Hash32,
    pub generation: u64,
    pub published_number: u64,
    pub published_hash: Hash32,
    pub idempotent: bool,
}

pub fn head_key(chain_id: u64) -> String {
    format!("chains/{}/heads/finalized.json", quantity(chain_id))
}

pub async fn publish(
    store: Arc<dyn ArchiveStore>,
    package: Package,
    gate: PublicationGate,
) -> Result<ArchiveOutcome> {
    let chain_id = package.segment.chain_id;
    let head_key = head_key(chain_id);
    let current = store.read_mutable(&head_key).await?;
    let previous = match current.as_ref() {
        Some(bytes) => Some(load_manifest_from_head(store.as_ref(), bytes, chain_id).await?),
        None => None,
    };

    let eligible = gate.eligible().min(package.segment.end_block);
    if let Some((_, manifest)) = previous.as_ref() {
        if manifest.input_sha256 == package.input_sha256 && manifest.published_number >= eligible {
            validate_gate(&package.segment, previous.as_ref(), &gate)?;
            return Ok(ArchiveOutcome {
                manifest: current_manifest(current.as_ref().unwrap())?,
                generation: manifest.generation,
                published_number: manifest.published_number,
                published_hash: manifest.published_hash,
                idempotent: true,
            });
        }
    }

    validate_chain_transition(&package, previous.as_ref())?;
    validate_gate(&package.segment, previous.as_ref(), &gate)?;
    if eligible < package.segment.start_block {
        bail!("publication gate does not make any package block eligible");
    }
    let segment = truncate_segment(package.segment.clone(), eligible)?;
    validate_code_references(store.as_ref(), previous.as_ref(), &segment).await?;

    let decoded_segment = canonical_json(&segment)?;
    let decoded_sha256 = Hash32::digest(&decoded_segment);
    let encoded_segment = zstd::encode_all(decoded_segment.as_slice(), 9)?;
    let segment_hash = Hash32::digest(&encoded_segment);
    let index = build_index(&segment, segment_hash, decoded_sha256);
    let encoded_index = canonical_json(&index)?;
    let index_hash = Hash32::digest(&encoded_index);

    store
        .put_immutable(&segment_hash.object_key(), &encoded_segment)
        .await?;
    store
        .put_immutable(&index_hash.object_key(), &encoded_index)
        .await?;

    let published = segment.blocks.last().unwrap();
    let (generation, anchor_number, anchor_hash, parent_manifest, mut descriptors) =
        match previous.as_ref() {
            None => (
                1,
                segment.start_block,
                segment.blocks.first().unwrap().hash,
                None,
                Vec::new(),
            ),
            Some((digest, manifest)) => (
                manifest.generation + 1,
                manifest.anchor_number,
                manifest.anchor_hash,
                Some(*digest),
                manifest.segments.clone(),
            ),
        };
    descriptors.push(SegmentDescriptor {
        start_block: segment.start_block,
        end_block: segment.end_block,
        segment_sha256: segment_hash,
        segment_size: encoded_segment.len() as u64,
        decoded_sha256,
        index_sha256: index_hash,
        index_size: encoded_index.len() as u64,
        encoding: "json+zstd".to_owned(),
    });
    descriptors.sort_by_key(|entry| (entry.start_block, entry.end_block, entry.segment_sha256));

    let manifest = Manifest {
        schema: MANIFEST_SCHEMA.to_owned(),
        generation,
        chain_id,
        genesis_hash: segment.genesis_hash,
        anchor_number,
        anchor_hash,
        published_number: published.number,
        published_hash: published.hash,
        published_state_root: published.state_root,
        parent_manifest,
        publication_gate: gate.description(),
        input_sha256: package.input_sha256,
        segments: descriptors,
    };
    let manifest_bytes = canonical_json(&manifest)?;
    let manifest_hash = Hash32::digest(&manifest_bytes);
    store
        .put_immutable(&manifest_hash.object_key(), &manifest_bytes)
        .await?;

    // The supplied checkpoint was validated against canonical input. Recheck immediately
    // before the only visibility-changing operation, then CAS the old head version.
    validate_gate(&package.segment, previous.as_ref(), &gate)?;
    let head = Head {
        schema: 1,
        chain_id: quantity(chain_id),
        generation,
        manifest: manifest_hash,
        number: quantity(published.number),
        hash: published.hash,
    };
    let head_bytes = canonical_json(&head)?;
    store
        .compare_and_swap(&head_key, current.as_ref(), &head_bytes)
        .await?;

    Ok(ArchiveOutcome {
        manifest: manifest_hash,
        generation,
        published_number: published.number,
        published_hash: published.hash,
        idempotent: false,
    })
}

fn validate_chain_transition(
    package: &Package,
    previous: Option<&(Hash32, Manifest)>,
) -> Result<()> {
    match (previous, package.mode) {
        (None, Mode::Delta) => bail!("first publication must be an anchor package"),
        (None, Mode::Anchor) => Ok(()),
        (Some(_), Mode::Anchor) => bail!("canonical conflict: archive already has an anchor"),
        (Some((_, manifest)), Mode::Delta) => {
            if manifest.chain_id != package.segment.chain_id
                || manifest.genesis_hash != package.segment.genesis_hash
            {
                bail!("package chain identity does not match archive");
            }
            if package.preceding_number != Some(manifest.published_number) {
                bail!(
                    "canonical gap/conflict: package preceding number does not match published head"
                );
            }
            if package.preceding_hash != Some(manifest.published_hash) {
                bail!("canonical conflict: preceding block hash differs from published head");
            }
            Ok(())
        }
    }
}

fn validate_gate(
    segment: &Segment,
    previous: Option<&(Hash32, Manifest)>,
    gate: &PublicationGate,
) -> Result<()> {
    let expected = match gate {
        PublicationGate::Finalized { number, hash } => (*number, *hash),
        PublicationGate::FixedOffset {
            observed_number,
            observed_hash,
            ..
        } => (*observed_number, *observed_hash),
    };
    let actual = segment
        .blocks
        .iter()
        .find(|block| block.number == expected.0)
        .map(|block| block.hash)
        .or_else(|| {
            previous.and_then(|(_, manifest)| {
                (manifest.published_number == expected.0).then_some(manifest.published_hash)
            })
        });
    if actual != Some(expected.1) {
        bail!("publication checkpoint hash is unavailable or not canonical");
    }
    let eligible = gate.eligible();
    if let Some((_, manifest)) = previous {
        if eligible < manifest.published_number {
            bail!("publication gate is behind the existing immutable head");
        }
    }
    Ok(())
}

fn truncate_segment(mut segment: Segment, eligible: u64) -> Result<Segment> {
    segment.blocks.retain(|event| event.number <= eligible);
    segment.accounts.retain(|event| event.block <= eligible);
    segment.storage.retain(|event| event.block <= eligible);
    segment
        .code
        .retain(|event| event.first_seen_block <= eligible);
    let last = segment
        .blocks
        .last()
        .ok_or_else(|| anyhow!("publication produced an empty segment"))?;
    segment.end_block = last.number;
    Ok(segment)
}

async fn validate_code_references(
    store: &dyn ArchiveStore,
    previous: Option<&(Hash32, Manifest)>,
    segment: &Segment,
) -> Result<()> {
    let mut available: BTreeMap<Hash32, u64> = segment
        .code
        .iter()
        .map(|blob| (blob.code_hash, blob.first_seen_block))
        .collect();
    if let Some((_, manifest)) = previous {
        for descriptor in &manifest.segments {
            let prior = load_segment(store, descriptor).await?;
            for blob in prior.code {
                available
                    .entry(blob.code_hash)
                    .and_modify(|first_seen| *first_seen = (*first_seen).min(blob.first_seen_block))
                    .or_insert(blob.first_seen_block);
            }
        }
    }
    let empty_code = {
        let digest = Keccak256::digest([]);
        let mut hash = [0; 32];
        hash.copy_from_slice(&digest);
        Hash32(hash)
    };
    for account in &segment.accounts {
        if account.exists
            && account.code_hash != empty_code
            && !available
                .get(&account.code_hash)
                .is_some_and(|first_seen| *first_seen <= account.block)
        {
            bail!(
                "account references bytecode {} unavailable at block {}",
                account.code_hash,
                account.block
            );
        }
    }
    Ok(())
}

fn build_index(segment: &Segment, segment_hash: Hash32, decoded_sha256: Hash32) -> SegmentIndex {
    let mut entries: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
    let mut add = |namespace: &str, key: String, block: u64| {
        let bounds = entries
            .entry((namespace.to_owned(), key))
            .or_insert((block, block));
        bounds.0 = bounds.0.min(block);
        bounds.1 = bounds.1.max(block);
    };
    for block in &segment.blocks {
        add(
            "block",
            format!("{:016x}:{}", block.number, block.hash),
            block.number,
        );
    }
    for event in &segment.accounts {
        add("account", event.address.to_string(), event.block);
    }
    for event in &segment.storage {
        add(
            "storage",
            format!(
                "{}:{:016x}:{}",
                event.address, event.incarnation, event.slot
            ),
            event.block,
        );
    }
    for blob in &segment.code {
        add("code", blob.code_hash.to_string(), blob.first_seen_block);
    }
    SegmentIndex {
        schema: INDEX_SCHEMA.to_owned(),
        segment: segment_hash,
        decoded_sha256,
        start_block: segment.start_block,
        end_block: segment.end_block,
        entries: entries
            .into_iter()
            .map(|((namespace, key), (min_block, max_block))| IndexEntry {
                namespace,
                key,
                min_block,
                max_block,
            })
            .collect(),
    }
}

pub async fn load_head_manifest(
    store: &dyn ArchiveStore,
    chain_id: u64,
) -> Result<(Head, Hash32, Manifest)> {
    let versioned = store
        .read_mutable(&head_key(chain_id))
        .await?
        .ok_or_else(|| anyhow!("archive has no published head"))?;
    let head: Head = serde_json::from_slice(&versioned.bytes).context("parse head")?;
    let (digest, manifest) = load_manifest_from_head(store, &versioned, chain_id).await?;
    Ok((head, digest, manifest))
}

async fn load_manifest_from_head(
    store: &dyn ArchiveStore,
    head_bytes: &VersionedBytes,
    expected_chain_id: u64,
) -> Result<(Hash32, Manifest)> {
    let head: Head = serde_json::from_slice(&head_bytes.bytes).context("parse head")?;
    if head.schema != 1 || head.chain_id != quantity(expected_chain_id) {
        bail!("head schema or chain identity mismatch");
    }
    let bytes = store.get(&head.manifest.object_key()).await?;
    if Hash32::digest(&bytes) != head.manifest {
        bail!("manifest digest mismatch");
    }
    let manifest: Manifest = serde_json::from_slice(&bytes).context("parse manifest")?;
    if manifest.schema != MANIFEST_SCHEMA
        || manifest.chain_id != expected_chain_id
        || manifest.generation != head.generation
        || manifest.published_number.to_string().is_empty()
        || quantity(manifest.published_number) != head.number
        || manifest.published_hash != head.hash
    {
        bail!("manifest does not match committed head");
    }
    Ok((head.manifest, manifest))
}

fn current_manifest(head: &VersionedBytes) -> Result<Hash32> {
    let head: Head = serde_json::from_slice(&head.bytes)?;
    Ok(head.manifest)
}

pub async fn load_segment(
    store: &dyn ArchiveStore,
    descriptor: &SegmentDescriptor,
) -> Result<Segment> {
    let encoded = store.get(&descriptor.segment_sha256.object_key()).await?;
    decode_segment(descriptor, &encoded)
}

pub fn decode_segment(descriptor: &SegmentDescriptor, encoded: &[u8]) -> Result<Segment> {
    if encoded.len() as u64 != descriptor.segment_size
        || Hash32::digest(encoded) != descriptor.segment_sha256
    {
        bail!("segment encoded length or digest mismatch");
    }
    if descriptor.encoding != "json+zstd" {
        bail!("unsupported segment encoding {}", descriptor.encoding);
    }
    let mut decoded = Vec::new();
    zstd::Decoder::new(encoded)?
        .take(512 * 1024 * 1024)
        .read_to_end(&mut decoded)?;
    if Hash32::digest(&decoded) != descriptor.decoded_sha256 {
        bail!("segment decoded digest mismatch");
    }
    let segment: Segment = serde_json::from_slice(&decoded).context("parse segment")?;
    if segment.schema != SEGMENT_SCHEMA
        || segment.start_block != descriptor.start_block
        || segment.end_block != descriptor.end_block
    {
        bail!("segment metadata mismatch");
    }
    Ok(segment)
}

pub async fn verify_index(
    store: &dyn ArchiveStore,
    descriptor: &SegmentDescriptor,
) -> Result<SegmentIndex> {
    let bytes = store.get(&descriptor.index_sha256.object_key()).await?;
    decode_index(descriptor, &bytes)
}

pub fn decode_index(descriptor: &SegmentDescriptor, bytes: &[u8]) -> Result<SegmentIndex> {
    if bytes.len() as u64 != descriptor.index_size
        || Hash32::digest(bytes) != descriptor.index_sha256
    {
        bail!("index length or digest mismatch");
    }
    let index: SegmentIndex = serde_json::from_slice(bytes).context("parse index")?;
    if index.schema != INDEX_SCHEMA
        || index.segment != descriptor.segment_sha256
        || index.decoded_sha256 != descriptor.decoded_sha256
        || index.start_block != descriptor.start_block
        || index.end_block != descriptor.end_block
    {
        bail!("index metadata mismatch");
    }
    if !index
        .entries
        .windows(2)
        .all(|pair| (&pair[0].namespace, &pair[0].key) < (&pair[1].namespace, &pair[1].key))
    {
        bail!("index entries are not strictly sorted");
    }
    Ok(index)
}
