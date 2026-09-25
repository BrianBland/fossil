//! Create a complete block-zero anchor from a chain's genesis allocation and block header.

use crate::format::{parse_quantity, parse_u256, quantity, quantity_u256, Address, Hash32};
use crate::normalized::{read_package, Mode};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
use std::str::FromStr;

/// Generate a normalized exhaustive block-zero package without accessing an object store.
pub fn genesis_anchor(genesis_bytes: &[u8], block_bytes: &[u8]) -> Result<Vec<u8>> {
    let genesis: Value =
        serde_json::from_slice(genesis_bytes).context("parse genesis allocation")?;
    let block_response: Value = serde_json::from_slice(block_bytes).context("parse block zero")?;
    let block = block_response.get("result").unwrap_or(&block_response);
    let chain_id = genesis["config"]["chainId"]
        .as_u64()
        .context("genesis config.chainId is missing")?;
    if block["number"].as_str() != Some("0x0") {
        bail!("anchor block must be block zero");
    }
    let hash = parse_hash(&block["hash"])?;
    let parent = parse_hash(&block["parentHash"])?;
    if parent != Hash32([0; 32]) {
        bail!("genesis block must have a zero parent hash");
    }
    let root = parse_hash(&block["stateRoot"])?;
    let timestamp = parse_quantity(
        block["timestamp"]
            .as_str()
            .context("genesis block missing timestamp")?,
    )?;
    let alloc = genesis["alloc"]
        .as_object()
        .context("genesis allocation is missing")?;
    if alloc.is_empty() {
        bail!("genesis allocation must not be empty");
    }
    let mut accounts = Vec::with_capacity(alloc.len());
    let mut storage = Vec::new();
    let mut codes: BTreeMap<Hash32, String> = BTreeMap::new();
    let empty_code = Hash32(Keccak256::digest([]).into());
    for (raw_address, state) in alloc {
        let address = Address::from_str(&format!("0x{raw_address}"))?;
        let nonce = state.get("nonce").and_then(Value::as_str).unwrap_or("0x0");
        let balance = state
            .get("balance")
            .and_then(Value::as_str)
            .unwrap_or("0x0");
        let nonce = quantity(parse_quantity(nonce)?);
        let balance = quantity_u256(&parse_u256(balance)?);
        let code = state.get("code").and_then(Value::as_str).unwrap_or("0x");
        let code_bytes = crate::format::parse_data(code)?;
        let code_hash = if code_bytes.is_empty() {
            empty_code
        } else {
            let digest = Hash32(Keccak256::digest(&code_bytes).into());
            if let Some(old) = codes.insert(digest, code.to_owned()) {
                if old != code {
                    bail!("distinct genesis bytecode shares a code hash");
                }
            }
            digest
        };
        accounts.push((address, nonce, balance, code_hash));
        if let Some(slots) = state.get("storage") {
            let slots = slots
                .as_object()
                .context("genesis account storage is not an object")?;
            for (raw_slot, raw_value) in slots {
                let slot = Hash32::from_str(raw_slot)?;
                let value = parse_hash(raw_value)?;
                if value != Hash32([0; 32]) {
                    storage.push((address, slot, value));
                }
            }
        }
    }
    accounts.sort_unstable_by_key(|(address, ..)| *address);
    storage.sort_unstable_by_key(|(address, slot, _)| (*address, *slot));
    let event_count = accounts.len() + storage.len() + codes.len();
    let mut output = Vec::new();
    append(
        &mut output,
        json!({
            "type":"header", "schema":"fossil-export/1", "chain_id":quantity(chain_id),
            "genesis_hash":hash, "mode":"anchor", "anchor_number":"0x0", "anchor_hash":hash
        }),
    )?;
    append(
        &mut output,
        json!({
            "type":"block", "number":"0x0", "hash":hash, "parent_hash":parent,
            "state_root":root, "timestamp":quantity(timestamp)
        }),
    )?;
    for (address, nonce, balance, code_hash) in accounts {
        append(
            &mut output,
            json!({
                "type":"account", "block":"0x0", "address":address, "exists":true,
                "incarnation":"0x0", "nonce":nonce, "balance":balance, "code_hash":code_hash
            }),
        )?;
    }
    for (address, slot, value) in storage {
        append(
            &mut output,
            json!({
                "type":"storage", "block":"0x0", "address":address,
                "incarnation":"0x0", "slot":slot, "value":value
            }),
        )?;
    }
    for (code_hash, bytes) in codes {
        append(
            &mut output,
            json!({"type":"code", "code_hash":code_hash, "bytes":bytes}),
        )?;
    }
    append(
        &mut output,
        json!({
            "type":"block_end", "number":"0x0", "event_count":quantity(event_count as u64)
        }),
    )?;
    append(
        &mut output,
        json!({
            "type":"trailer", "end_number":"0x0", "end_hash":hash, "block_count":"0x1"
        }),
    )?;
    let parsed = read_package(&output)?;
    if parsed.mode != Mode::Anchor || parsed.segment.accounts.len() != alloc.len() {
        return Err(anyhow!(
            "generated genesis package did not retain every account"
        ));
    }
    Ok(output)
}

fn parse_hash(value: &Value) -> Result<Hash32> {
    Hash32::from_str(value.as_str().context("expected 32-byte hex hash")?)
}

fn append(out: &mut Vec<u8>, value: Value) -> Result<()> {
    serde_json::to_writer(&mut *out, &value)?;
    out.push(b'\n');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_exact_anchor_and_skips_zero_storage() {
        let address = "11".repeat(20);
        let slot = format!("0x{}", "22".repeat(32));
        let one = format!("0x{}1", "0".repeat(63));
        let zero = format!("0x{}", "00".repeat(32));
        let genesis = json!({
            "config":{"chainId":8453},
            "alloc":{(address):{"balance":"0x0", "code":"0x6000", "storage":{
                (slot):one,
                (format!("0x{}", "33".repeat(32))):zero
            }}}
        });
        let hash = format!("0x{}", "aa".repeat(32));
        let block = json!({"result":{
            "number":"0x0", "hash":hash, "parentHash":format!("0x{}", "00".repeat(32)),
            "stateRoot":format!("0x{}", "bb".repeat(32)), "timestamp":"0x2"
        }});
        let bytes = genesis_anchor(
            &serde_json::to_vec(&genesis).unwrap(),
            &serde_json::to_vec(&block).unwrap(),
        )
        .unwrap();
        let package = read_package(&bytes).unwrap();
        assert_eq!(package.segment.accounts.len(), 1);
        assert_eq!(package.segment.storage.len(), 1);
        assert_eq!(package.segment.code.len(), 1);
        assert_eq!(package.segment.blocks[0].number, 0);
        assert_eq!(package.segment.storage[0].value[31], 1);
    }
}
