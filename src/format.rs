use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

pub const SCHEMA: &str = "fossil-export/1";
pub const SEGMENT_SCHEMA: &str = "fossil-segment/1";
pub const ZERO_HASH: Hash32 = Hash32([0; 32]);

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Hash32(pub [u8; 32]);

impl Hash32 {
    pub fn digest(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        let mut value = [0; 32];
        value.copy_from_slice(&digest);
        Self(value)
    }

    pub fn object_key(self) -> String {
        let hex = hex::encode(self.0);
        format!("objects/sha256/{}/{}", &hex[..2], hex)
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl From<Hash32> for String {
    fn from(value: Hash32) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for Hash32 {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        parse_fixed::<32>(&value).map(Self)
    }
}

impl FromStr for Hash32 {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        parse_fixed::<32>(s).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Address(pub [u8; 20]);

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl From<Address> for String {
    fn from(value: Address) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for Address {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        parse_fixed::<20>(&value).map(Self)
    }
}

impl FromStr for Address {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        parse_fixed::<20>(s).map(Self)
    }
}

pub fn parse_data(value: &str) -> Result<Vec<u8>> {
    let raw = value
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("hex data must have a 0x prefix"))?;
    if raw.len() % 2 != 0 || raw.bytes().any(|c| c.is_ascii_uppercase()) {
        bail!("hex data must be lowercase and have an even number of digits");
    }
    hex::decode(raw).context("invalid hex data")
}

pub fn parse_quantity(value: &str) -> Result<u64> {
    let raw = value
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("quantity must have a 0x prefix"))?;
    if raw.is_empty()
        || (raw.len() > 1 && raw.starts_with('0'))
        || raw.bytes().any(|c| c.is_ascii_uppercase())
    {
        bail!("quantity is not canonical");
    }
    u64::from_str_radix(raw, 16).context("quantity exceeds u64")
}

pub fn parse_u256(value: &str) -> Result<[u8; 32]> {
    let raw = value
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("quantity must have a 0x prefix"))?;
    if raw.is_empty()
        || raw.len() > 64
        || (raw.len() > 1 && raw.starts_with('0'))
        || raw.bytes().any(|c| c.is_ascii_uppercase())
    {
        bail!("invalid canonical U256 quantity");
    }
    let padded = format!("{raw:0>64}");
    parse_fixed_raw::<32>(&padded)
}

pub fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}

pub fn quantity_u256(value: &[u8; 32]) -> String {
    let first = value.iter().position(|byte| *byte != 0);
    match first {
        None => "0x0".to_owned(),
        Some(index) => {
            let encoded = hex::encode(&value[index..]);
            format!("0x{}", encoded.trim_start_matches('0'))
        }
    }
}

pub fn data32(value: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(value))
}

fn parse_fixed<const N: usize>(value: &str) -> Result<[u8; N]> {
    let raw = value
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("fixed hex value must have a 0x prefix"))?;
    parse_fixed_raw(raw)
}

fn parse_fixed_raw<const N: usize>(raw: &str) -> Result<[u8; N]> {
    if raw.len() != N * 2 || raw.bytes().any(|c| c.is_ascii_uppercase()) {
        bail!("expected exactly {} lowercase hex bytes", N);
    }
    let decoded = hex::decode(raw).context("invalid hex")?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("expected exactly {N} bytes"))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub number: u64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
    pub state_root: Hash32,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountEvent {
    pub block: u64,
    pub address: Address,
    pub exists: bool,
    pub incarnation: u64,
    pub nonce: u64,
    pub balance: [u8; 32],
    pub code_hash: Hash32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageEvent {
    pub block: u64,
    pub address: Address,
    pub incarnation: u64,
    pub slot: Hash32,
    pub value: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodeBlob {
    pub first_seen_block: u64,
    pub code_hash: Hash32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub schema: String,
    pub chain_id: u64,
    pub genesis_hash: Hash32,
    pub start_block: u64,
    pub end_block: u64,
    pub blocks: Vec<BlockMeta>,
    pub accounts: Vec<AccountEvent>,
    pub storage: Vec<StorageEvent>,
    pub code: Vec<CodeBlob>,
}
