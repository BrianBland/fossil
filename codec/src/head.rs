//! The mutable tiered head (inline manifest), key layout and account encoding.

use crate::run::ObjectRef;
use crate::Hash32;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// L0 runs folded per compaction step.
pub const L0_TRIGGER: usize = 4;
/// Publication refuses to append beyond this many uncompacted L0 runs.
pub const MAX_L0_BACKLOG: usize = 16;
pub const FANOUT: u64 = 8;
pub const MAX_LEVELS: usize = 8;
/// Pre-fetch bound for the head object.
pub const MAX_HEAD: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ref {
    pub digest: Hash32,
    pub length: u32,
}
impl From<ObjectRef> for Ref {
    fn from(value: ObjectRef) -> Self {
        Self {
            digest: value.digest,
            length: value.length,
        }
    }
}
impl From<Ref> for ObjectRef {
    fn from(value: Ref) -> Self {
        Self {
            digest: value.digest,
            length: value.length,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub start: u64,
    pub end: u64,
    pub weight: u64,
    pub keys: u64,
    pub shards: u32,
    pub root: Ref,
    /// Hex of the 581-byte run root, verified against `root`.
    pub root_bytes: String,
}
impl Run {
    /// Root reference and bytes, ready for `RunReader::decode`.
    pub fn input(&self) -> Result<(ObjectRef, Vec<u8>)> {
        Ok((self.root.into(), hex::decode(&self.root_bytes)?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Head {
    pub magic: String,
    pub chain_id: u64,
    pub genesis: Hash32,
    pub number: u64,
    pub hash: Hash32,
    pub state_root: Hash32,
    pub input_sha256: Hash32,
    /// Immutable copy of the head this one replaced (publication or compaction).
    pub parent: Option<Ref>,
    pub l0: Vec<Run>,
    /// `levels[i]` weight is a multiple of `unit(i)` and below `unit(i) * FANOUT`.
    pub levels: Vec<Option<Run>>,
}
impl Head {
    /// Runs oldest first.
    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.levels.iter().rev().flatten().chain(self.l0.iter())
    }

    /// Parse and fully validate head bytes: bounds, level weights, contiguous
    /// coverage from genesis to `number`, and every inline run root digest.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let head: Self = serde_json::from_slice(bytes)?;
        if head.magic != "FTVH1" || head.l0.len() > MAX_L0_BACKLOG || head.levels.len() > MAX_LEVELS
        {
            bail!("invalid tiered head");
        }
        if head.l0.iter().any(|run| run.weight != 1) {
            bail!("invalid L0 run weight");
        }
        for (index, level) in head.levels.iter().enumerate() {
            if let Some(run) = level {
                if run.weight == 0
                    || run.weight % unit(index) != 0
                    || run.weight >= unit(index) * FANOUT
                {
                    bail!("invalid tiered level weight");
                }
            }
        }
        let mut expected_start = 0;
        let mut count = 0;
        for run in head.runs() {
            let root = hex::decode(&run.root_bytes)?;
            if run.start != expected_start
                || run.start > run.end
                || !run.shards.is_power_of_two()
                || root.len() != run.root.length as usize
                || Hash32::digest(&root) != run.root.digest
            {
                bail!("invalid tiered run interval, root or summary");
            }
            expected_start = run.end + 1;
            count += 1;
        }
        if count == 0 || expected_start != head.number + 1 {
            bail!("tiered runs do not cover genesis through the head");
        }
        Ok(head)
    }
}

pub fn unit(level: usize) -> u64 {
    L0_TRIGGER as u64 * FANOUT.pow(level as u32)
}

/// Mutable head path relative to the store prefix (decimal chain ID).
pub fn head_key(chain_id: u64) -> String {
    format!("chains/{chain_id}/heads/tiered-v1.json")
}

pub fn account_key(address: [u8; 20]) -> Vec<u8> {
    let mut key = vec![1];
    key.extend_from_slice(&address);
    key
}
pub fn storage_key(address: [u8; 20], incarnation: u64, slot: [u8; 32]) -> Vec<u8> {
    let mut key = vec![2];
    key.extend_from_slice(&address);
    key.extend_from_slice(&incarnation.to_be_bytes());
    key.extend_from_slice(&slot);
    key
}
pub fn code_key(hash: [u8; 32]) -> Vec<u8> {
    let mut key = vec![3];
    key.extend_from_slice(&hash);
    key
}

/// An account post-state version. Tombstones have zero nonce, balance and code hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountState {
    pub exists: bool,
    pub incarnation: u64,
    pub nonce: u64,
    pub balance: [u8; 32],
    pub code_hash: [u8; 32],
}
impl AccountState {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![u8::from(self.exists)];
        out.extend_from_slice(&self.incarnation.to_be_bytes());
        out.extend_from_slice(&self.nonce.to_be_bytes());
        out.extend_from_slice(&self.balance);
        out.extend_from_slice(&self.code_hash);
        out
    }

    pub fn decode(value: &[u8]) -> Result<Self> {
        if value.len() != 81 || value[0] > 1 {
            bail!("invalid full-key account poststate");
        }
        let state = Self {
            exists: value[0] == 1,
            incarnation: u64::from_be_bytes(value[1..9].try_into()?),
            nonce: u64::from_be_bytes(value[9..17].try_into()?),
            balance: value[17..49].try_into()?,
            code_hash: value[49..81].try_into()?,
        };
        if !state.exists
            && (state.nonce != 0 || state.balance != [0; 32] || state.code_hash != [0; 32])
        {
            bail!("invalid account tombstone");
        }
        Ok(state)
    }
}
