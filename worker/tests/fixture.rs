use base64::Engine as _;
use fossil_worker::core::*;
use futures::executor::block_on;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;

struct MemoryStore {
    objects: HashMap<String, Vec<u8>>,
    gets: Cell<usize>,
}
impl ObjectStore for MemoryStore {
    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        self.gets.set(self.gets.get() + 1);
        let bytes = self
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| ArchiveError::Unavailable("missing".into()))?;
        if bytes.len() > maximum {
            return Err(ArchiveError::Integrity);
        }
        Ok(bytes)
    }
}

fn fixture() -> (MemoryStore, Value) {
    let value: Value =
        serde_json::from_str(include_str!("../testdata/archive-fixture.json")).unwrap();
    let objects = value["objects"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, encoded)| {
            (
                format!("fossil-demo/{key}"),
                base64::engine::general_purpose::STANDARD
                    .decode(encoded.as_str().unwrap())
                    .unwrap(),
            )
        })
        .collect();
    (
        MemoryStore {
            objects,
            gets: Cell::new(0),
        },
        value,
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TestRef {
    digest: [u8; 32],
    length: u32,
}
type TestPointer = (TestRef, u32, u32);
type TestEpoch = (
    [TestRef; 32],
    [TestRef; 128],
    BTreeMap<Vec<u8>, TestPointer>,
);
type PartitionEntries = BTreeMap<(usize, usize), Vec<(Vec<u8>, TestPointer)>>;

fn uleb(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}
fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    uleb(out, value.len() as u64);
    out.extend_from_slice(value);
}
fn object_ref(out: &mut Vec<u8>, value: TestRef) {
    out.extend_from_slice(&value.digest);
    out.extend_from_slice(&value.length.to_be_bytes());
}
fn add_object(objects: &mut HashMap<String, Vec<u8>>, value: Vec<u8>) -> TestRef {
    let digest: [u8; 32] = Sha256::digest(&value).into();
    let text = hex::encode(digest);
    objects.insert(
        format!("fossil-demo/objects/sha256/{}/{text}", &text[..2]),
        value.clone(),
    );
    TestRef {
        digest,
        length: value.len() as u32,
    }
}
fn compressed(magic: &[u8; 4], decoded: &[u8]) -> Vec<u8> {
    let encoded = zstd::encode_all(Cursor::new(decoded), 9).unwrap();
    let mut out = magic.to_vec();
    out.push(1);
    out.extend_from_slice(&(decoded.len() as u32).to_be_bytes());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.extend_from_slice(&encoded);
    out
}
fn state_key(namespace: u8, address: [u8; 20], incarnation: Option<u64>) -> Vec<u8> {
    let mut key = vec![namespace];
    key.extend_from_slice(&address);
    if let Some(incarnation) = incarnation {
        key.extend_from_slice(&incarnation.to_be_bytes());
        key.extend_from_slice(&[0; 31]);
        key.push(1);
    }
    key
}
fn route(key: &[u8]) -> (usize, usize, [u8; 12]) {
    let full = Sha256::digest(key);
    let routed = Sha256::digest(&key[1..21]);
    (
        routed[0] as usize >> 1,
        full[12] as usize >> 5,
        full[..12].try_into().unwrap(),
    )
}
fn account(exists: bool, incarnation: u64, balance: u8) -> Vec<u8> {
    let mut out = vec![u8::from(exists)];
    uleb(&mut out, incarnation);
    uleb(&mut out, 1);
    out.push(u8::from(balance != 0));
    if balance != 0 {
        out.push(balance);
    }
    out.extend_from_slice(
        &hex::decode("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").unwrap(),
    );
    out
}
fn storage(value: u8) -> Vec<u8> {
    vec![1, value]
}

fn build_epoch(
    objects: &mut HashMap<String, Vec<u8>>,
    block: u64,
    changes: BTreeMap<Vec<u8>, Vec<u8>>,
) -> TestEpoch {
    let mut groups: BTreeMap<usize, BTreeMap<Vec<u8>, Vec<u8>>> = BTreeMap::new();
    for (key, value) in changes {
        groups
            .entry(route(&key).0 >> 2)
            .or_default()
            .insert(key, value);
    }
    let mut data = [TestRef::default(); 32];
    let mut pointers = BTreeMap::new();
    let mut by_partition: BTreeMap<usize, Vec<([u8; 12], u32, TestRef)>> = BTreeMap::new();
    for (group, runs) in groups {
        let mut body = Vec::new();
        uleb(&mut body, runs.len() as u64);
        for (run_index, (key, value)) in runs.iter().enumerate() {
            bytes(&mut body, key);
            uleb(&mut body, 1);
            uleb(&mut body, block);
            bytes(&mut body, value);
            by_partition.entry(route(key).0).or_default().push((
                route(key).2,
                run_index as u32,
                TestRef::default(),
            ));
        }
        let reference = add_object(objects, compressed(b"FSED", &body));
        data[group] = reference;
        for (run_index, key) in runs.keys().enumerate() {
            pointers.insert(key.clone(), (reference, run_index as u32, 0));
            let entries = by_partition.get_mut(&route(key).0).unwrap();
            entries
                .iter_mut()
                .filter(|entry| entry.1 == run_index as u32)
                .for_each(|entry| entry.2 = reference);
        }
    }
    let mut indexes = [TestRef::default(); 128];
    for (partition, mut entries) in by_partition {
        entries.sort_by_key(|entry| (entry.0, entry.1));
        let data_ref = entries[0].2;
        let mut index = b"FSEI".to_vec();
        index.push(1);
        index.push(partition as u8);
        index.extend_from_slice(&block.to_be_bytes());
        uleb(&mut index, 1);
        object_ref(&mut index, data_ref);
        uleb(&mut index, entries.len() as u64);
        for (fingerprint, run_index, _) in entries {
            index.extend_from_slice(&fingerprint);
            uleb(&mut index, 0);
            index.push(0);
            index.extend_from_slice(&run_index.to_be_bytes());
        }
        indexes[partition] = add_object(objects, index);
    }
    (data, indexes, pointers)
}

fn directory(
    window_start: u64,
    checkpoint: TestRef,
    epochs: &[(u64, [TestRef; 32], [TestRef; 128])],
    dummy: TestRef,
) -> Vec<u8> {
    let mut out = b"FSER".to_vec();
    out.push(1);
    out.extend_from_slice(&window_start.to_be_bytes());
    object_ref(&mut out, checkpoint);
    out.push(epochs.len() as u8);
    for (block, data, indexes) in epochs {
        out.extend_from_slice(&block.to_be_bytes());
        out.extend_from_slice(&block.to_be_bytes());
        object_ref(&mut out, dummy);
        for reference in data {
            object_ref(&mut out, *reference);
        }
        for reference in indexes {
            object_ref(&mut out, *reference);
        }
    }
    out
}

fn completed_window_fixture(
    corrupt_catalog_range: bool,
    corrupt_checkpoint_boundary: bool,
) -> (MemoryStore, [u8; 32]) {
    let mut objects = HashMap::new();
    let a = [0x11; 20];
    let b = [0x22; 20];
    let c = [0x33; 20];
    let mut old = BTreeMap::new();
    for (address, balance) in [(a, 1), (b, 2), (c, 3)] {
        old.insert(state_key(1, address, None), account(true, 1, balance));
        old.insert(state_key(2, address, Some(1)), storage(balance + 40));
    }
    let (old_data, old_indexes, pointers) = build_epoch(&mut objects, 0, old);
    let dummy = old_data
        .iter()
        .copied()
        .find(|item| item.length != 0)
        .unwrap();

    let mut partition_entries = PartitionEntries::new();
    for (key, pointer) in pointers {
        let routed = route(&key);
        partition_entries
            .entry((routed.0, routed.1))
            .or_default()
            .push((key, pointer));
    }
    let mut partition_shards: BTreeMap<usize, [TestRef; 8]> = BTreeMap::new();
    for ((partition, subshard), mut entries) in partition_entries {
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut body = Vec::new();
        uleb(&mut body, entries.len() as u64);
        for (key, (object, run, version)) in entries {
            bytes(&mut body, &key);
            object_ref(&mut body, object);
            body.extend_from_slice(&run.to_be_bytes());
            body.extend_from_slice(&version.to_be_bytes());
        }
        let shard = add_object(&mut objects, compressed(b"FSEP", &body));
        partition_shards.entry(partition).or_default()[subshard] = shard;
    }
    let mut partitions = [TestRef::default(); 128];
    for (partition, shards) in partition_shards {
        let mut manifest = b"FSPS".to_vec();
        manifest.push(1);
        for shard in shards {
            object_ref(&mut manifest, shard);
        }
        partitions[partition] = add_object(&mut objects, manifest);
    }
    let mut checkpoint = b"FSEM".to_vec();
    checkpoint.push(1);
    checkpoint.extend_from_slice(
        &(if corrupt_checkpoint_boundary {
            62u64
        } else {
            63
        })
        .to_be_bytes(),
    );
    for partition in partitions {
        object_ref(&mut checkpoint, partition);
    }
    let checkpoint_ref = add_object(&mut objects, checkpoint);

    let empty_data = [TestRef::default(); 32];
    let empty_indexes = [TestRef::default(); 128];
    let mut completed_epochs = vec![(0, old_data, old_indexes)];
    for block in 1..64 {
        completed_epochs.push((block, empty_data, empty_indexes));
    }
    let completed_ref = add_object(
        &mut objects,
        directory(0, TestRef::default(), &completed_epochs, dummy),
    );

    let mut active = BTreeMap::new();
    active.insert(state_key(1, a, None), account(false, 1, 0));
    active.insert(state_key(1, c, None), account(true, 2, 9));
    active.insert(state_key(2, c, Some(2)), storage(99));
    let (active_data, active_indexes, _) = build_epoch(&mut objects, 64, active);
    let active_ref = add_object(
        &mut objects,
        directory(
            64,
            checkpoint_ref,
            &[(64, active_data, active_indexes)],
            dummy,
        ),
    );

    let mut chunk = b"FSWC".to_vec();
    chunk.push(1);
    uleb(&mut chunk, 1);
    chunk.extend_from_slice(&0u64.to_be_bytes());
    chunk.extend_from_slice(&(if corrupt_catalog_range { 62u64 } else { 63 }).to_be_bytes());
    object_ref(&mut chunk, completed_ref);
    let chunk_ref = add_object(&mut objects, chunk);
    let mut root = b"FSEW".to_vec();
    root.push(1);
    uleb(&mut root, 1);
    root.extend_from_slice(&0u64.to_be_bytes());
    object_ref(&mut root, chunk_ref);
    let root_ref = add_object(&mut objects, root);

    let mut commit = b"FSEC".to_vec();
    commit.push(1);
    commit.extend_from_slice(&1u64.to_be_bytes());
    commit.extend_from_slice(&8453u64.to_be_bytes());
    commit.extend_from_slice(&[1; 32]);
    commit.extend_from_slice(&0u64.to_be_bytes());
    commit.extend_from_slice(&[2; 32]);
    commit.extend_from_slice(&[3; 32]);
    commit.extend_from_slice(&64u64.to_be_bytes());
    commit.extend_from_slice(&[4; 32]);
    object_ref(&mut commit, TestRef::default());
    commit.push(1);
    commit.extend_from_slice(&[5; 32]);
    commit.extend_from_slice(&64u64.to_be_bytes());
    object_ref(&mut commit, active_ref);
    object_ref(&mut commit, root_ref);
    assert_eq!(commit.len(), 314);
    let commit_ref = add_object(&mut objects, commit);
    let mut head = b"FSEH".to_vec();
    head.push(1);
    head.extend_from_slice(&1u64.to_be_bytes());
    head.extend_from_slice(&8453u64.to_be_bytes());
    head.extend_from_slice(&64u64.to_be_bytes());
    head.extend_from_slice(&[3; 32]);
    object_ref(&mut head, commit_ref);
    objects.insert("fossil-demo/chains/0x2105/heads/finalized.bin".into(), head);
    (
        MemoryStore {
            objects,
            gets: Cell::new(0),
        },
        commit_ref.digest,
    )
}

#[test]
fn deterministic_native_fixture_queries_match_format_v1_golden_bytes_and_incarnation() {
    block_on(async {
        let (store, fixture) = fixture();
        let expected = &fixture["expected"];
        let address = parse_address(&expected["address"]).unwrap();
        let code_address = parse_address(&expected["codeAddress"]).unwrap();
        let slot = parse_slot(&expected["slot"]).unwrap();
        let mut reader = Reader::load(&store, "fossil-demo/".into(), 8453)
            .await
            .unwrap();

        assert_eq!(reader.commit.published_number, 102);
        assert_eq!(
            reader
                .account_at(address, 100)
                .await
                .unwrap()
                .unwrap()
                .nonce,
            7
        );
        assert_eq!(
            quantity_bytes(
                &reader
                    .account_at(address, 100)
                    .await
                    .unwrap()
                    .unwrap()
                    .balance
            ),
            "0x3e8"
        );
        assert_eq!(
            quantity_bytes(
                &reader
                    .account_at(address, 102)
                    .await
                    .unwrap()
                    .unwrap()
                    .balance
            ),
            "0x7d0"
        );
        assert_eq!(reader.storage_at(address, slot, 100).await.unwrap()[31], 42);
        assert_eq!(
            reader.storage_at(address, slot, 102).await.unwrap(),
            [0; 32]
        );
        assert_eq!(
            format!(
                "0x{}",
                hex::encode(reader.code_at(code_address, 102).await.unwrap())
            ),
            expected["code"].as_str().unwrap()
        );
    });
}

#[test]
fn selector_and_corruption_fail_closed() {
    block_on(async {
        let (store, fixture_value) = fixture();
        let reader = Reader::load(&store, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        for selector in ["latest", "safe", "finalized"] {
            assert_eq!(
                reader
                    .resolve_selector(&Value::String(selector.into()))
                    .unwrap(),
                102
            );
        }
        assert_eq!(
            reader
                .resolve_selector(&Value::String("0x64".into()))
                .unwrap(),
            100
        );
        assert!(matches!(
            reader.resolve_selector(&Value::String("0x064".into())),
            Err(ArchiveError::InvalidParams(_))
        ));
        assert!(matches!(
            reader.resolve_selector(&Value::String("pending".into())),
            Err(ArchiveError::InvalidParams(_))
        ));
        assert_eq!(
            reader
                .resolve_selector(&serde_json::json!({"blockNumber":"0x64"}))
                .unwrap(),
            100
        );
        assert_eq!(
            reader
                .resolve_selector(
                    &serde_json::json!({"blockNumber":"latest","requireCanonical":true})
                )
                .unwrap(),
            102
        );
        for invalid in [
            serde_json::json!({"blockHash":"0x00"}),
            serde_json::json!({"blockNumber":"0x64","blockHash":"0x00"}),
            serde_json::json!({"blockNumber":"0x64","requireCanonical":"yes"}),
            serde_json::json!({"blockNumber":"0x64","extra":true}),
        ] {
            assert!(matches!(
                reader.resolve_selector(&invalid),
                Err(ArchiveError::InvalidParams(_))
            ));
        }
        assert!(matches!(
            reader.resolve_selector(&Value::String("earliest".into())),
            Err(ArchiveError::Unavailable(_))
        ));
        let mut genesis_reader = reader;
        genesis_reader.commit.anchor_number = 0;
        genesis_reader.commit.active_window_start = 0;
        assert_eq!(
            genesis_reader
                .resolve_selector(&Value::String("earliest".into()))
                .unwrap(),
            0
        );
        genesis_reader.commit.finalized = false;
        assert!(matches!(
            genesis_reader.resolve_selector(&Value::String("safe".into())),
            Err(ArchiveError::Unavailable(_))
        ));
        drop(fixture_value);

        let (mut store, _) = fixture();
        let head = &store.objects["fossil-demo/chains/0x2105/heads/finalized.bin"];
        let digest = hex::encode(&head[61..93]);
        let commit_key = format!("fossil-demo/objects/sha256/{}/{digest}", &digest[..2]);
        store.objects.get_mut(&commit_key).unwrap()[10] ^= 1;
        assert_eq!(
            Reader::load(&store, "fossil-demo/".into(), 8453)
                .await
                .err()
                .unwrap(),
            ArchiveError::Integrity
        );

        let (mut malformed, _) = fixture();
        malformed
            .objects
            .get_mut("fossil-demo/chains/0x2105/heads/finalized.bin")
            .unwrap()[93..97]
            .fill(0);
        assert_eq!(
            Reader::load(&malformed, "fossil-demo/".into(), 8453)
                .await
                .err()
                .unwrap(),
            ArchiveError::Integrity
        );
    });
}

#[test]
fn completed_window_catalog_and_checkpoint_golden_cover_65_epochs() {
    block_on(async {
        let (store, commit_digest) = completed_window_fixture(false, false);
        assert_eq!(
            hex::encode(commit_digest),
            "5e60880acc06fbfc02d32d530ae08760f8f83edf0ef6c9f45a129e3c27149957"
        );
        let a = [0x11; 20];
        let b = [0x22; 20];
        let c = [0x33; 20];
        let slot = {
            let mut value = [0; 32];
            value[31] = 1;
            value
        };
        let mut reader = Reader::load(&store, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        assert_eq!(
            reader
                .resolve_selector(&Value::String("0x0".into()))
                .unwrap(),
            0
        );
        let old = reader.account_at(a, 0).await.unwrap().unwrap();
        assert_eq!(old.balance[31], 1);
        assert_eq!(reader.storage_at(a, slot, 0).await.unwrap()[31], 41);

        let unchanged = reader.account_at(b, 64).await.unwrap().unwrap();
        assert_eq!(unchanged.balance[31], 2);
        assert_eq!(reader.storage_at(b, slot, 64).await.unwrap()[31], 42);
        assert!(reader.account_at(a, 64).await.unwrap().is_none());
        assert_eq!(reader.storage_at(a, slot, 64).await.unwrap(), [0; 32]);
        assert_eq!(reader.storage_at(c, slot, 64).await.unwrap()[31], 99);
        assert!(reader.account_at([0x44; 20], 64).await.unwrap().is_none());

        // A cold storage query includes head+commit, active directory, account
        // checkpoint fallback, and incarnation-qualified storage fallback.
        store.gets.set(0);
        let mut cold = Reader::load(&store, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        assert_eq!(cold.storage_at(b, slot, 64).await.unwrap()[31], 42);
        assert_eq!(store.gets.get(), 8);

        let (corrupt, _) = completed_window_fixture(true, false);
        let mut reader = Reader::load(&corrupt, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        assert!(matches!(
            reader.account_at(a, 0).await,
            Err(ArchiveError::Integrity)
        ));

        let (corrupt, _) = completed_window_fixture(false, true);
        let mut reader = Reader::load(&corrupt, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        assert!(matches!(
            reader.account_at(b, 64).await,
            Err(ArchiveError::Integrity)
        ));
    });
}
