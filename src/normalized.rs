use crate::format::{
    parse_data, parse_quantity, parse_u256, AccountEvent, Address, BlockMeta, CodeBlob, Hash32,
    Segment, StorageEvent, SCHEMA, SEGMENT_SCHEMA, ZERO_HASH,
};
use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use sha3::{Digest, Keccak256};
use std::collections::{BTreeSet, HashSet};
use std::io::Read;

#[derive(Clone, Debug)]
pub struct Package {
    pub mode: Mode,
    pub preceding_number: Option<u64>,
    pub preceding_hash: Option<Hash32>,
    pub input_sha256: Hash32,
    pub segment: Segment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Anchor,
    Delta,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        schema: String,
        chain_id: String,
        genesis_hash: Hash32,
        mode: String,
        anchor_number: Option<String>,
        anchor_hash: Option<Hash32>,
        preceding_number: Option<String>,
        preceding_hash: Option<Hash32>,
    },
    Block {
        number: String,
        hash: Hash32,
        parent_hash: Hash32,
        state_root: Hash32,
        timestamp: String,
    },
    Account {
        block: String,
        address: Address,
        exists: bool,
        incarnation: String,
        nonce: Option<String>,
        balance: Option<String>,
        code_hash: Option<Hash32>,
    },
    Storage {
        block: String,
        address: Address,
        incarnation: String,
        slot: Hash32,
        value: Hash32,
    },
    Code {
        code_hash: Hash32,
        bytes: String,
    },
    BlockEnd {
        number: String,
        event_count: String,
    },
    Trailer {
        end_number: String,
        end_hash: Hash32,
        block_count: String,
    },
}

pub fn read_package(bytes: &[u8]) -> Result<Package> {
    let decoded = if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        let mut output = Vec::new();
        zstd::Decoder::new(bytes)
            .context("open zstd input")?
            .take(1 << 30)
            .read_to_end(&mut output)
            .context("decode zstd input")?;
        output
    } else {
        bytes.to_vec()
    };
    parse_package(&decoded, Hash32::digest(bytes))
}

fn parse_package(decoded: &[u8], input_sha256: Hash32) -> Result<Package> {
    let text = std::str::from_utf8(decoded).context("input is not UTF-8 JSONL")?;
    let mut records = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<Record>(line)
            .with_context(|| format!("invalid JSONL record on line {}", index + 1))?;
        records.push(record);
    }
    if records.len() < 3 {
        bail!("package requires header, block records, and trailer");
    }

    let (
        chain_id,
        genesis_hash,
        mode,
        anchor_number,
        anchor_hash,
        preceding_number,
        preceding_hash,
    ) = match records.remove(0) {
        Record::Header {
            schema,
            chain_id,
            genesis_hash,
            mode,
            anchor_number,
            anchor_hash,
            preceding_number,
            preceding_hash,
        } => {
            if schema != SCHEMA {
                bail!("unsupported input schema {schema}");
            }
            let mode = match mode.as_str() {
                "anchor" => Mode::Anchor,
                "delta" => Mode::Delta,
                _ => bail!("mode must be anchor or delta"),
            };
            (
                parse_quantity(&chain_id)?,
                genesis_hash,
                mode,
                anchor_number.map(|n| parse_quantity(&n)).transpose()?,
                anchor_hash,
                preceding_number.map(|n| parse_quantity(&n)).transpose()?,
                preceding_hash,
            )
        }
        _ => bail!("first record must be a header"),
    };

    match mode {
        Mode::Anchor if anchor_number.is_none() || anchor_hash.is_none() => {
            bail!("anchor header requires anchor_number and anchor_hash")
        }
        Mode::Delta if preceding_number.is_none() || preceding_hash.is_none() => {
            bail!("delta header requires preceding_number and preceding_hash")
        }
        _ => {}
    }

    let mut blocks: Vec<BlockMeta> = Vec::new();
    let mut accounts = Vec::new();
    let mut storage = Vec::new();
    let mut code = Vec::new();
    let mut current: Option<BlockMeta> = None;
    let mut event_count = 0_u64;
    let mut last_account = None;
    let mut last_storage = None;
    let mut account_keys = HashSet::new();
    let mut storage_keys = HashSet::new();
    let mut code_hashes = BTreeSet::new();
    let mut trailer = None;

    for record in records {
        if trailer.is_some() {
            bail!("trailer must be the final record");
        }
        match record {
            Record::Block {
                number,
                hash,
                parent_hash,
                state_root,
                timestamp,
            } => {
                if current.is_some() {
                    bail!("block_end is required before the next block");
                }
                let number = parse_quantity(&number)?;
                if let Some(previous) = blocks.last() {
                    if previous.number.checked_add(1) != Some(number) {
                        bail!("noncontiguous block {} after {}", number, previous.number);
                    }
                    if parent_hash != previous.hash {
                        bail!("parent hash mismatch at block {number}");
                    }
                } else if mode == Mode::Delta && parent_hash != preceding_hash.unwrap_or_default() {
                    bail!("first delta parent does not match preceding_hash");
                }
                current = Some(BlockMeta {
                    number,
                    hash,
                    parent_hash,
                    state_root,
                    timestamp: parse_quantity(&timestamp)?,
                });
                event_count = 0;
                last_account = None;
                last_storage = None;
                account_keys.clear();
                storage_keys.clear();
            }
            Record::Account {
                block,
                address,
                exists,
                incarnation,
                nonce,
                balance,
                code_hash,
            } => {
                let block = checked_current(&current, &block)?;
                if last_account.is_some_and(|last| address <= last) {
                    bail!("account records are not strictly address-sorted in block {block}");
                }
                if !account_keys.insert(address) {
                    bail!("duplicate account record in block {block}");
                }
                last_account = Some(address);
                let (nonce, balance, code_hash) = if exists {
                    (
                        parse_quantity(
                            &nonce.ok_or_else(|| anyhow!("live account missing nonce"))?,
                        )?,
                        parse_u256(
                            &balance.ok_or_else(|| anyhow!("live account missing balance"))?,
                        )?,
                        code_hash.ok_or_else(|| anyhow!("live account missing code_hash"))?,
                    )
                } else {
                    if nonce.is_some() || balance.is_some() || code_hash.is_some() {
                        bail!("account tombstone must omit nonce, balance, and code_hash");
                    }
                    (0, [0; 32], ZERO_HASH)
                };
                accounts.push(AccountEvent {
                    block,
                    address,
                    exists,
                    incarnation: parse_quantity(&incarnation)?,
                    nonce,
                    balance,
                    code_hash,
                });
                event_count += 1;
            }
            Record::Storage {
                block,
                address,
                incarnation,
                slot,
                value,
            } => {
                let block = checked_current(&current, &block)?;
                let incarnation = parse_quantity(&incarnation)?;
                let key = (address, incarnation, slot);
                if last_storage.is_some_and(|last| key <= last) {
                    bail!("storage records are not strictly key-sorted in block {block}");
                }
                if !storage_keys.insert(key) {
                    bail!("duplicate storage record in block {block}");
                }
                last_storage = Some(key);
                storage.push(StorageEvent {
                    block,
                    address,
                    incarnation,
                    slot,
                    value: value.0,
                });
                event_count += 1;
            }
            Record::Code { code_hash, bytes } => {
                let block = current
                    .as_ref()
                    .ok_or_else(|| anyhow!("code record before first block"))?
                    .number;
                let bytes = parse_data(&bytes)?;
                let digest = Keccak256::digest(&bytes);
                if digest.as_slice() != code_hash.0 {
                    bail!("bytecode does not match code_hash {code_hash}");
                }
                if !code_hashes.insert(code_hash) {
                    bail!("duplicate code blob {code_hash}");
                }
                code.push(CodeBlob {
                    first_seen_block: block,
                    code_hash,
                    bytes,
                });
                event_count += 1;
            }
            Record::BlockEnd {
                number,
                event_count: expected,
            } => {
                let number = checked_current(&current, &number)?;
                if parse_quantity(&expected)? != event_count {
                    bail!("event_count mismatch at block {number}");
                }
                blocks.push(
                    current
                        .take()
                        .ok_or_else(|| anyhow!("block_end without open block"))?,
                );
            }
            Record::Trailer {
                end_number,
                end_hash,
                block_count,
            } => {
                if current.is_some() {
                    bail!("block_end is required before the trailer");
                }
                trailer = Some((
                    parse_quantity(&end_number)?,
                    end_hash,
                    parse_quantity(&block_count)?,
                ));
            }
            Record::Header { .. } => bail!("header may only appear once"),
        }
    }

    let (trailer_number, trailer_hash, block_count) =
        trailer.ok_or_else(|| anyhow!("missing trailer"))?;
    let first = blocks
        .first()
        .ok_or_else(|| anyhow!("package has no blocks"))?;
    let last = blocks.last().unwrap();
    if trailer_number != last.number
        || trailer_hash != last.hash
        || block_count != blocks.len() as u64
    {
        bail!("trailer does not match package blocks");
    }
    if mode == Mode::Anchor
        && (anchor_number != Some(first.number) || anchor_hash != Some(first.hash))
    {
        bail!("anchor header does not match first block");
    }
    if first.number == 0 && first.hash != genesis_hash {
        bail!("genesis block hash does not match genesis_hash");
    }
    if mode == Mode::Delta
        && preceding_number.and_then(|number| number.checked_add(1)) != Some(first.number)
    {
        bail!("delta does not immediately follow preceding_number");
    }

    Ok(Package {
        mode,
        preceding_number,
        preceding_hash,
        input_sha256,
        segment: Segment {
            schema: SEGMENT_SCHEMA.to_owned(),
            chain_id,
            genesis_hash,
            start_block: first.number,
            end_block: last.number,
            blocks,
            accounts,
            storage,
            code,
        },
    })
}

fn checked_current(current: &Option<BlockMeta>, value: &str) -> Result<u64> {
    let number = parse_quantity(value)?;
    if current.as_ref().map(|block| block.number) != Some(number) {
        bail!("event block {number} does not match current block");
    }
    Ok(number)
}
