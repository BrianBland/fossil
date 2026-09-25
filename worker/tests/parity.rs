//! Worker/native parity: the native publisher and compactor build a real tiered
//! store on disk; the Worker core must answer every query exactly like the
//! native reader, within its request budget.

use fossil::format::{Address, Hash32};
use fossil::normalized::read_package;
use fossil::store::open_store;
use fossil::tiered;
use fossil::tiered::PublicationGate;
use fossil_worker::core::{ArchiveError, ObjectStore, Reader, Result};
use sha3::{Digest, Keccak256};
use std::cell::Cell;
use std::path::PathBuf;

const ADDRESSES: u8 = 6;
const SLOTS: u8 = 4;
const PACKAGES: u64 = 45;

struct DirStore {
    root: PathBuf,
    gets: Cell<u64>,
}
impl ObjectStore for DirStore {
    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        self.gets.set(self.gets.get() + 1);
        let bytes = std::fs::read(self.root.join(key))
            .map_err(|_| ArchiveError::Unavailable("missing".into()))?;
        if bytes.len() > maximum {
            return Err(ArchiveError::Integrity);
        }
        Ok(bytes)
    }
}

fn hash(number: u64) -> String {
    format!("0x{}", hex::encode(Hash32::digest(&number.to_be_bytes()).0))
}
fn address(index: u8) -> String {
    format!("0x{}", hex::encode([index + 1; 20]))
}
fn slot(index: u8) -> String {
    format!("0x{}", hex::encode([0xa0 + index; 32]))
}

/// Deterministic account/storage churn with a destroy and recreation.
fn package(number: u64, code_hash: &str, empty: &str) -> Vec<u8> {
    let mut lines = Vec::new();
    if number == 0 {
        lines.push(format!(r#"{{"type":"header","schema":"fossil-export/1","chain_id":"0x1","genesis_hash":"{g}","mode":"anchor","anchor_number":"0x0","anchor_hash":"{g}"}}"#, g = hash(0)));
    } else {
        lines.push(format!(r#"{{"type":"header","schema":"fossil-export/1","chain_id":"0x1","genesis_hash":"{}","mode":"delta","preceding_number":"0x{:x}","preceding_hash":"{}"}}"#, hash(0), number - 1, hash(number - 1)));
    }
    let parent = if number == 0 {
        format!("0x{}", "00".repeat(32))
    } else {
        hash(number - 1)
    };
    lines.push(format!(r#"{{"type":"block","number":"0x{number:x}","hash":"{}","parent_hash":"{parent}","state_root":"{}","timestamp":"0x{number:x}"}}"#, hash(number), hash(1000 + number)));
    let mut events = 0;
    let destroyed = number == 20;
    let incarnation = if number >= 21 { 1 } else { 0 };
    for index in 0..ADDRESSES {
        // Address 0 changes every block; the rest change rarely, so old runs matter.
        if number != 0
            && index != 0
            && !(number + index as u64).is_multiple_of(11)
            && !(index == 1 && (destroyed || number == 21))
        {
            continue;
        }
        let incarnation = if index == 1 { incarnation } else { 0 };
        if index == 1 && destroyed {
            lines.push(format!(r#"{{"type":"account","block":"0x{number:x}","address":"{}","exists":false,"incarnation":"0x0"}}"#, address(index)));
            events += 1;
            continue;
        }
        let code = if index % 2 == 0 { code_hash } else { empty };
        lines.push(format!(r#"{{"type":"account","block":"0x{number:x}","address":"{}","exists":true,"incarnation":"0x{incarnation:x}","nonce":"0x{number:x}","balance":"0x{:x}","code_hash":"{code}"}}"#, address(index), number * 10 + index as u64));
        events += 1;
        for s in 0..SLOTS {
            if (number + s as u64).is_multiple_of(3) {
                let value = if (number + s as u64).is_multiple_of(9) {
                    format!("0x{}", "00".repeat(32))
                } else {
                    hash(number * 100 + s as u64)
                };
                lines.push(format!(r#"{{"type":"storage","block":"0x{number:x}","address":"{}","incarnation":"0x{incarnation:x}","slot":"{}","value":"{value}"}}"#, address(index), slot(s)));
                events += 1;
            }
        }
    }
    if number == 0 {
        lines.push(format!(
            r#"{{"type":"code","code_hash":"{code_hash}","bytes":"0x6001"}}"#
        ));
        events += 1;
    }
    lines.push(format!(
        r#"{{"type":"block_end","number":"0x{number:x}","event_count":"0x{events:x}"}}"#
    ));
    lines.push(format!(
        r#"{{"type":"trailer","end_number":"0x{number:x}","end_hash":"{}","block_count":"0x1"}}"#,
        hash(number)
    ));
    lines.join("\n").into_bytes()
}

#[tokio::test]
async fn worker_matches_native_reader_after_compaction() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let code_hash = Hash32(Keccak256::digest([0x60, 0x01]).into()).to_string();
    let empty = Hash32(Keccak256::digest([]).into()).to_string();
    for number in 0..=PACKAGES {
        let package = read_package(&package(number, &code_hash, &empty)).unwrap();
        let gate = PublicationGate::Finalized {
            number,
            hash: hash(number).parse().unwrap(),
        };
        tiered::publish(store.as_ref(), &package, &gate)
            .await
            .unwrap();
        while tiered::compact_once(store.as_ref(), 1).await.unwrap() {}
    }
    let native = tiered::Reader::open(store.as_ref(), 1).await.unwrap();
    assert!(native.run_count() >= 3, "{} runs", native.run_count());
    let worker_store = DirStore {
        root: root.path().to_path_buf(),
        gets: Cell::new(0),
    };
    let mut checked = 0;
    for block in 0..=PACKAGES {
        for index in 0..=ADDRESSES {
            let raw = [index + 1; 20];
            let account = native
                .account(Address(raw), block)
                .await
                .unwrap()
                .filter(|a| a.exists);
            for s in 0..SLOTS {
                worker_store.gets.set(0);
                let worker = Reader::load(&worker_store, String::new(), 1).await.unwrap();
                let slot_bytes = [0xa0 + s; 32];
                let expected = native
                    .storage(Address(raw), Hash32(slot_bytes), block)
                    .await
                    .unwrap();
                assert_eq!(
                    worker.storage_at(raw, slot_bytes, block).await.unwrap(),
                    expected,
                    "block {block} address {index} slot {s}"
                );
                let runs = native.run_count() as u64;
                // head + one summary shard per run + fence/data for account and slot.
                assert!(
                    worker_store.gets.get() <= 1 + runs + 6,
                    "{} GETs over {runs} runs",
                    worker_store.gets.get()
                );
                checked += 1;
            }
            let worker = Reader::load(&worker_store, String::new(), 1).await.unwrap();
            let got = worker.account_at(raw, block).await.unwrap();
            assert_eq!(
                got.map(|a| (a.nonce, a.balance, a.incarnation)),
                account.map(|a| (a.nonce, a.balance, a.incarnation))
            );
            let code = native.code(Address(raw), block).await.unwrap();
            assert_eq!(worker.code_at(raw, block).await.unwrap(), code);
        }
    }
    assert!(checked > 1000);
    let worker = Reader::load(&worker_store, String::new(), 1).await.unwrap();
    assert!(worker
        .resolve_selector(&serde_json::json!(format!("0x{:x}", PACKAGES + 1)))
        .is_err());
    assert_eq!(
        worker
            .resolve_selector(&serde_json::json!("latest"))
            .unwrap(),
        PACKAGES
    );
}
