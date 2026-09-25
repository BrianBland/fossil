//! Platform-neutral, bounded tiered v1 reader used by the Worker and native tests.
//!
//! One GET reads the head (whose manifest is inline); each run is then probed
//! newest first through its no-false-negative summary shard, and positives are
//! verified in the run's exact fence tree. Every GET is charged to a hard
//! per-request budget before it is issued.

use fossil_codec::head::{
    account_key, code_key, head_key, storage_key, AccountState, Head, Run, MAX_HEAD,
};
use fossil_codec::run::{ObjectRef, RunReader};
use fossil_codec::summary::{shard_path, Probe, Shard, ShardKind, MAX_SHARD_BYTES};
use fossil_codec::Hash32;
use sha3::{Digest as _, Keccak256};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

const MAX_REMOTE_GETS: u64 = 192;
const MAX_REQUEST_BYTES: u64 = 128 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 1024 * 1024;
const EMPTY_CODE_HASH: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveError {
    Unavailable(String),
    Integrity,
    Limit,
    InvalidParams(String),
    Backend,
}
impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ArchiveError {}
/// Store errors pass through the codec unchanged; codec errors are integrity failures.
impl From<anyhow::Error> for ArchiveError {
    fn from(error: anyhow::Error) -> Self {
        error.downcast().unwrap_or(ArchiveError::Integrity)
    }
}

pub type Result<T> = std::result::Result<T, ArchiveError>;

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Account {
    pub exists: bool,
    pub incarnation: u64,
    pub nonce: u64,
    pub balance: [u8; 32],
    pub code_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestStats {
    pub gets: u64,
    pub fetched_bytes: u64,
}

pub struct Reader<'a, S: ObjectStore> {
    store: &'a S,
    prefix: String,
    head: Head,
    runs: Vec<(Run, RunReader)>,
    gets: Cell<u64>,
    fetched: Cell<u64>,
    shards: RefCell<HashMap<String, Rc<Shard>>>,
}

impl<'a, S: ObjectStore> Reader<'a, S> {
    /// One request's pinned view. `prefix` ends with `/` (or is empty).
    pub async fn load(store: &'a S, prefix: String, chain_id: u64) -> Result<Self> {
        let reader = Self {
            store,
            head: placeholder_head(),
            runs: Vec::new(),
            gets: Cell::new(0),
            fetched: Cell::new(0),
            shards: RefCell::new(HashMap::new()),
            prefix,
        };
        let bytes = reader
            .get(
                &format!("{}{}", reader.prefix, head_key(chain_id)),
                MAX_HEAD,
            )
            .await?;
        let head = Head::decode(&bytes)?;
        if head.chain_id != chain_id {
            return Err(ArchiveError::Integrity);
        }
        let mut runs = Vec::new();
        for run in head.runs() {
            let (reference, root) = run.input()?;
            runs.push((run.clone(), RunReader::decode(reference, &root)?));
        }
        Ok(Self {
            head,
            runs,
            ..reader
        })
    }

    pub fn published_number(&self) -> u64 {
        self.head.number
    }

    pub fn stats(&self) -> RequestStats {
        RequestStats {
            gets: self.gets.get(),
            fetched_bytes: self.fetched.get(),
        }
    }

    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let gets = self.gets.get() + 1;
        let fetched = self.fetched.get() + maximum as u64;
        if gets > MAX_REMOTE_GETS || fetched > MAX_REQUEST_BYTES {
            return Err(ArchiveError::Limit);
        }
        self.gets.set(gets);
        self.fetched.set(fetched);
        self.store.get(key, maximum).await
    }

    async fn object(&self, reference: ObjectRef) -> Result<Vec<u8>> {
        let length = reference.length as usize;
        if length == 0 || length > MAX_OBJECT_BYTES {
            return Err(ArchiveError::Integrity);
        }
        let key = format!("{}{}", self.prefix, reference.digest.object_key());
        let bytes = self.get(&key, length).await?;
        if bytes.len() != length || Hash32::digest(&bytes) != reference.digest {
            return Err(ArchiveError::Integrity);
        }
        Ok(bytes)
    }

    async fn shard(&self, run: &Run, kind: ShardKind, index: u32) -> Result<Rc<Shard>> {
        let path = format!(
            "{}{}",
            self.prefix,
            shard_path(run.root.digest, run.shards, kind, index)
        );
        if let Some(shard) = self.shards.borrow().get(&path) {
            return Ok(shard.clone());
        }
        let bytes = self.get(&path, MAX_SHARD_BYTES).await?;
        let shard = Rc::new(Shard::decode(
            run.root.digest,
            kind,
            index,
            run.shards,
            &bytes,
        )?);
        self.shards.borrow_mut().insert(path, shard.clone());
        Ok(shard)
    }

    async fn may_contain(&self, run: &Run, key: &[u8]) -> Result<bool> {
        let probe = Probe::new(key, run.shards);
        let address = self
            .shard(run, ShardKind::Address, probe.address_index)
            .await?;
        if address.contains(key) {
            return Ok(true);
        }
        if !address.contains(&probe.marker) {
            return Ok(false);
        }
        Ok(self
            .shard(run, ShardKind::Spill, probe.spill_index)
            .await?
            .contains(key))
    }

    async fn lookup(&self, key: &[u8], block: u64) -> Result<Option<Vec<u8>>> {
        for (run, reader) in self.runs.iter().rev() {
            if run.start > block || !self.may_contain(run, key).await? {
                continue;
            }
            let value = reader
                .lookup(key, block.min(run.end), |reference| async move {
                    self.object(reference).await.map_err(anyhow::Error::new)
                })
                .await?;
            if value.is_some() {
                return Ok(value);
            }
        }
        Ok(None)
    }

    pub async fn account_at(&self, address: [u8; 20], block: u64) -> Result<Option<Account>> {
        let Some(value) = self.lookup(&account_key(address), block).await? else {
            return Ok(None);
        };
        let state = AccountState::decode(&value)?;
        Ok(Some(Account {
            exists: state.exists,
            incarnation: state.incarnation,
            nonce: state.nonce,
            balance: state.balance,
            code_hash: state.code_hash,
        })
        .filter(|account| account.exists))
    }

    pub async fn storage_at(
        &self,
        address: [u8; 20],
        slot: [u8; 32],
        block: u64,
    ) -> Result<[u8; 32]> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok([0; 32]);
        };
        match self
            .lookup(&storage_key(address, account.incarnation, slot), block)
            .await?
        {
            Some(value) => value.try_into().map_err(|_| ArchiveError::Integrity),
            None => Ok([0; 32]),
        }
    }

    pub async fn code_at(&self, address: [u8; 20], block: u64) -> Result<Vec<u8>> {
        let Some(account) = self.account_at(address, block).await? else {
            return Ok(Vec::new());
        };
        if account.code_hash == EMPTY_CODE_HASH {
            return Ok(Vec::new());
        }
        let bytes = self
            .lookup(&code_key(account.code_hash), block)
            .await?
            .ok_or(ArchiveError::Integrity)?;
        if bytes.len() > MAX_OBJECT_BYTES
            || Keccak256::digest(&bytes).as_slice() != account.code_hash
        {
            return Err(ArchiveError::Integrity);
        }
        Ok(bytes)
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
                if object
                    .get("requireCanonical")
                    .is_some_and(|value| !value.is_boolean())
                {
                    return Err(ArchiveError::InvalidParams(
                        "requireCanonical must be boolean".into(),
                    ));
                }
                object.get("blockNumber").ok_or_else(|| {
                    ArchiveError::InvalidParams("block-hash selectors are not supported".into())
                })?
            }
            value => value,
        };
        let text = selector
            .as_str()
            .ok_or_else(|| ArchiveError::InvalidParams("invalid block selector".into()))?;
        let block = match text {
            "latest" | "safe" | "finalized" => self.head.number,
            "earliest" => 0,
            "pending" => {
                return Err(ArchiveError::InvalidParams(
                    "pending is not supported".into(),
                ))
            }
            value => parse_quantity(value)?,
        };
        if block > self.head.number {
            return Err(ArchiveError::Unavailable(
                "block is outside the published archive range".into(),
            ));
        }
        Ok(block)
    }
}

/// Stand-in used only while the real head is being fetched.
fn placeholder_head() -> Head {
    Head {
        magic: String::new(),
        chain_id: 0,
        genesis: Hash32::default(),
        number: 0,
        hash: Hash32::default(),
        state_root: Hash32::default(),
        input_sha256: Hash32::default(),
        parent: None,
        l0: Vec::new(),
        levels: Vec::new(),
    }
}

pub fn parse_address(value: &serde_json::Value) -> Result<[u8; 20]> {
    let invalid = || ArchiveError::InvalidParams("address must be 20-byte hex data".into());
    let text = value.as_str().ok_or_else(invalid)?;
    if text.len() != 42 || !text.starts_with("0x") {
        return Err(invalid());
    }
    hex::decode(&text[2..])
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())
}
pub fn parse_slot(value: &serde_json::Value) -> Result<[u8; 32]> {
    let invalid = || ArchiveError::InvalidParams("slot must be at most 32 bytes of hex".into());
    let text = value.as_str().ok_or_else(invalid)?;
    if !text.starts_with("0x")
        || text.len() < 3
        || text.len() > 66
        || !text[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid());
    }
    let mut digits = text[2..].to_string();
    if digits.len() % 2 == 1 {
        digits.insert(0, '0');
    }
    let bytes = hex::decode(digits).map_err(|_| invalid())?;
    let mut out = [0; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}
pub fn parse_quantity(value: &str) -> Result<u64> {
    let invalid =
        || ArchiveError::InvalidParams("block number must be a canonical hex quantity".into());
    let digits = value.strip_prefix("0x").ok_or_else(invalid)?;
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid());
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
