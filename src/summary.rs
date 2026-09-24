//! No-false-negative run membership summaries.
//!
//! Each run gets `shards` address-routed Bloom shards plus the same number of
//! key-routed spill shards. Every key of an address (its account record and all
//! of its storage) is inserted into the address's shard, so one GET answers both
//! halves of an account-plus-storage lookup. Addresses with more than
//! `HEAVY_GROUP` keys in a run instead place a marker in their address shard and
//! put their keys into spill shards, keeping every shard bounded.
//!
//! Shards live at deterministic paths derived from the run root digest, so the
//! reader needs no directory GET. Each shard binds its run digest, kind, index
//! and shard count, and ends with a SHA-256 of its preceding bytes.

use crate::format::Hash32;
use anyhow::{bail, Result};

const MAGIC: &[u8; 4] = b"FTSM";
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 4 + 1 + 32 + 1 + 4 + 4;
/// Target keys per shard; 16 bits per key keeps shards near 64 KiB.
const SHARD_KEYS: u64 = 32 * 1024;
const BITS_PER_KEY: u64 = 16;
const HASHES: u64 = 11;
const HEAVY_GROUP: usize = 4096;
/// Hard cap on encoded shard size (the reader's pre-fetch bound).
pub const MAX_SHARD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardKind {
    Address = 0,
    Spill = 1,
}

/// Group prefix: the tag byte plus the 20-byte address (or whole short key).
fn group(key: &[u8]) -> &[u8] {
    &key[..key.len().min(21)]
}
fn route(group: &[u8]) -> &[u8] {
    &group[1.min(group.len())..]
}
fn marker(group: &[u8]) -> Vec<u8> {
    let mut item = vec![0xff];
    item.extend_from_slice(group);
    item
}
fn shard_index(bytes: &[u8], shards: u32) -> u32 {
    let digest = Hash32::digest(bytes).0;
    u32::from_be_bytes(digest[..4].try_into().unwrap()) & (shards - 1)
}
fn spill_index(digest: &Hash32, shards: u32) -> u32 {
    u32::from_be_bytes(digest.0[16..20].try_into().unwrap()) & (shards - 1)
}
fn insert(bits: &mut [u8], digest: &Hash32) {
    let total = bits.len() as u64 * 8;
    let (h1, h2) = hashes(digest);
    for i in 0..HASHES {
        let bit = h1.wrapping_add(i.wrapping_mul(h2)) % total;
        bits[(bit / 8) as usize] |= 1 << (bit % 8);
    }
}
fn contains(bits: &[u8], digest: &Hash32) -> bool {
    let total = bits.len() as u64 * 8;
    let (h1, h2) = hashes(digest);
    (0..HASHES).all(|i| {
        let bit = h1.wrapping_add(i.wrapping_mul(h2)) % total;
        bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
    })
}
fn hashes(digest: &Hash32) -> (u64, u64) {
    let h1 = u64::from_be_bytes(digest.0[..8].try_into().unwrap());
    let h2 = u64::from_be_bytes(digest.0[8..16].try_into().unwrap()) | 1;
    (h1, h2)
}

/// Shard count for a run expected to hold `keys` distinct keys (power of two).
pub fn shard_count(keys: u64) -> u32 {
    keys.div_ceil(SHARD_KEYS).max(1).next_power_of_two() as u32
}

/// The shard count is part of the path: identical run roots built with
/// different summary sizing must not collide under create-only puts.
pub fn shard_path(run: Hash32, shards: u32, kind: ShardKind, index: u32) -> String {
    let tag = match kind {
        ShardKind::Address => 'a',
        ShardKind::Spill => 's',
    };
    format!(
        "summaries/{}/{shards:08x}/{tag}{index:08x}",
        hex::encode(run.0)
    )
}

/// Streaming builder. Keys must be distinct and arrive in ascending order.
pub struct SummaryBuilder {
    shards: u32,
    address: Vec<Vec<u8>>,
    spill: Vec<Vec<u8>>,
    group: Vec<u8>,
    pending: Vec<Hash32>,
    heavy: bool,
    keys: u64,
}

impl SummaryBuilder {
    /// `expected_keys` may overestimate (merges); underestimates only raise the
    /// false-positive rate, never cause false negatives.
    // ponytail: all shard bitsets stay in memory (~4 bytes per expected key);
    // spill to disk if a single merged run ever exceeds a few hundred million keys.
    pub fn new(expected_keys: u64) -> Result<Self> {
        let shards = shard_count(expected_keys);
        let per_shard = expected_keys.div_ceil(shards as u64).max(64);
        let bytes = (per_shard * BITS_PER_KEY).div_ceil(8) as usize;
        if HEADER_BYTES + bytes + 32 > MAX_SHARD_BYTES {
            bail!("summary shard exceeds the 1 MiB bound");
        }
        Ok(Self {
            shards,
            address: vec![vec![0; bytes]; shards as usize],
            spill: vec![vec![0; bytes]; shards as usize],
            group: Vec::new(),
            pending: Vec::new(),
            heavy: false,
            keys: 0,
        })
    }

    pub fn add(&mut self, key: &[u8]) {
        self.keys += 1;
        if group(key) != self.group.as_slice() {
            self.flush();
            self.group = group(key).to_vec();
        }
        let digest = Hash32::digest(key);
        if self.heavy {
            let index = spill_index(&digest, self.shards);
            insert(&mut self.spill[index as usize], &digest);
            return;
        }
        self.pending.push(digest);
        if self.pending.len() > HEAVY_GROUP {
            self.heavy = true;
            let index = shard_index(route(&self.group), self.shards);
            insert(
                &mut self.address[index as usize],
                &Hash32::digest(&marker(&self.group)),
            );
            for digest in std::mem::take(&mut self.pending) {
                let index = spill_index(&digest, self.shards);
                insert(&mut self.spill[index as usize], &digest);
            }
        }
    }

    fn flush(&mut self) {
        if !self.heavy && !self.pending.is_empty() {
            let index = shard_index(route(&self.group), self.shards);
            for digest in std::mem::take(&mut self.pending) {
                insert(&mut self.address[index as usize], &digest);
            }
        }
        self.pending.clear();
        self.heavy = false;
    }

    pub fn keys(&self) -> u64 {
        self.keys
    }

    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Encode all shards for the run whose root digest is `run`.
    pub fn finish(mut self, run: Hash32) -> Vec<(String, Vec<u8>)> {
        self.flush();
        let shards = self.shards;
        let encode = |kind: ShardKind, index: u32, bits: Vec<u8>| {
            let mut out = Vec::with_capacity(HEADER_BYTES + bits.len() + 32);
            out.extend_from_slice(MAGIC);
            out.push(VERSION);
            out.extend_from_slice(&run.0);
            out.push(kind as u8);
            out.extend_from_slice(&index.to_be_bytes());
            out.extend_from_slice(&shards.to_be_bytes());
            out.extend_from_slice(&bits);
            let check = Hash32::digest(&out);
            out.extend_from_slice(&check.0);
            (shard_path(run, shards, kind, index), out)
        };
        let address = self
            .address
            .into_iter()
            .enumerate()
            .map(|(index, bits)| encode(ShardKind::Address, index as u32, bits));
        let spill = self
            .spill
            .into_iter()
            .enumerate()
            .map(|(index, bits)| encode(ShardKind::Spill, index as u32, bits));
        address.chain(spill).collect()
    }
}

/// A verified Bloom shard.
#[derive(Clone, Debug)]
pub struct Shard {
    bits: Vec<u8>,
}

impl Shard {
    pub fn decode(
        run: Hash32,
        kind: ShardKind,
        index: u32,
        shards: u32,
        bytes: &[u8],
    ) -> Result<Self> {
        if bytes.len() <= HEADER_BYTES + 32 || bytes.len() > MAX_SHARD_BYTES {
            bail!("invalid summary shard length");
        }
        let (body, check) = bytes.split_at(bytes.len() - 32);
        if Hash32::digest(body).0 != check
            || &body[..4] != MAGIC
            || body[4] != VERSION
            || body[5..37] != run.0
            || body[37] != kind as u8
            || body[38..42] != index.to_be_bytes()
            || body[42..46] != shards.to_be_bytes()
        {
            bail!("summary shard does not match its run");
        }
        Ok(Self {
            bits: body[HEADER_BYTES..].to_vec(),
        })
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        contains(&self.bits, &Hash32::digest(item))
    }
}

/// Where the reader must look for `key` in a run with `shards` shards.
pub struct Probe {
    pub address_index: u32,
    pub spill_index: u32,
    pub marker: Vec<u8>,
}

impl Probe {
    pub fn new(key: &[u8], shards: u32) -> Self {
        let group = group(key);
        Self {
            address_index: shard_index(route(group), shards),
            spill_index: spill_index(&Hash32::digest(key), shards),
            marker: marker(group),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tag: u8, address: u8, slot: u32) -> Vec<u8> {
        let mut key = vec![tag];
        key.extend_from_slice(&[address; 20]);
        if tag == 2 {
            key.extend_from_slice(&slot.to_be_bytes());
        }
        key
    }

    #[test]
    fn no_false_negatives_for_light_and_heavy_addresses() -> Result<()> {
        let mut keys = vec![key(1, 1, 0), key(1, 2, 0)];
        keys.extend((0..10_000).map(|slot| key(2, 1, slot)));
        keys.extend((0..10).map(|slot| key(2, 2, slot)));
        // Enough light accounts to force multiple routed shards.
        keys.extend((0..40_000_u32).map(|index| {
            let mut key = vec![1; 21];
            key[17..].copy_from_slice(&index.to_be_bytes());
            key
        }));
        keys.sort();
        keys.dedup();
        let mut builder = SummaryBuilder::new(keys.len() as u64)?;
        for key in &keys {
            builder.add(key);
        }
        let shards = builder.shards();
        assert!(shards > 1);
        let run = Hash32([9; 32]);
        let objects: std::collections::HashMap<_, _> = builder.finish(run).into_iter().collect();
        let mut decoded = std::collections::HashMap::new();
        for kind in [ShardKind::Address, ShardKind::Spill] {
            for index in 0..shards {
                let bytes = &objects[&shard_path(run, shards, kind, index)];
                decoded.insert(
                    (kind as u8, index),
                    Shard::decode(run, kind, index, shards, bytes)?,
                );
            }
        }
        let load = |kind: ShardKind, index| Ok::<_, anyhow::Error>(&decoded[&(kind as u8, index)]);
        let present = |key: &[u8]| -> Result<bool> {
            let probe = Probe::new(key, shards);
            let address = load(ShardKind::Address, probe.address_index)?;
            if address.contains(key) {
                return Ok(true);
            }
            Ok(address.contains(&probe.marker)
                && load(ShardKind::Spill, probe.spill_index)?.contains(key))
        };
        for key in &keys {
            assert!(present(key)?);
        }
        let false_positives = (20_000..30_000)
            .filter(|slot| present(&key(2, 1, *slot)).unwrap())
            .count();
        assert!(false_positives < 50, "{false_positives}");
        assert!(!present(&key(2, 3, 0))? || !present(&key(2, 3, 1))?);
        let mut tampered = objects[&shard_path(run, shards, ShardKind::Address, 0)].clone();
        tampered[HEADER_BYTES] ^= 1;
        assert!(Shard::decode(run, ShardKind::Address, 0, shards, &tampered).is_err());
        assert!(Shard::decode(
            Hash32([8; 32]),
            ShardKind::Address,
            0,
            shards,
            &objects[&shard_path(run, shards, ShardKind::Address, 0)]
        )
        .is_err());
        Ok(())
    }
}
