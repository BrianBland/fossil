use base64::Engine as _;
use fossil_worker::core::*;
use futures::executor::block_on;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;

struct MemoryStore(HashMap<String, Vec<u8>>);
impl ObjectStore for MemoryStore {
    async fn get(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        let bytes = self
            .0
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
    (MemoryStore(objects), value)
}

fn checkpoint_fixture() -> MemoryStore {
    let (mut store, _) = fixture();
    let head_key = "fossil-demo/chains/0x2105/heads/finalized.bin";
    let mut head = store.0[head_key].clone();
    let commit_digest = hex::encode(&head[61..93]);
    let commit_key = format!(
        "fossil-demo/objects/sha256/{}/{commit_digest}",
        &commit_digest[..2]
    );
    let mut commit = store.0.remove(&commit_key).unwrap();
    let directory_digest = hex::encode(&commit[242..274]);
    let directory_key = format!(
        "fossil-demo/objects/sha256/{}/{directory_digest}",
        &directory_digest[..2]
    );
    let mut directory = store.0.remove(&directory_key).unwrap();

    directory[13..45].fill(0xbb);
    directory[45..49].copy_from_slice(&1u32.to_be_bytes());
    let new_directory_digest = Sha256::digest(&directory);
    commit[242..274].copy_from_slice(&new_directory_digest);
    let new_commit_digest = Sha256::digest(&commit);
    head[61..93].copy_from_slice(&new_commit_digest);

    let directory_hex = hex::encode(new_directory_digest);
    let commit_hex = hex::encode(new_commit_digest);
    store.0.insert(
        format!(
            "fossil-demo/objects/sha256/{}/{directory_hex}",
            &directory_hex[..2]
        ),
        directory,
    );
    store.0.insert(
        format!(
            "fossil-demo/objects/sha256/{}/{commit_hex}",
            &commit_hex[..2]
        ),
        commit,
    );
    store.0.insert(head_key.into(), head);
    store
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
        let head = &store.0["fossil-demo/chains/0x2105/heads/finalized.bin"];
        let digest = hex::encode(&head[61..93]);
        let commit_key = format!("fossil-demo/objects/sha256/{}/{digest}", &digest[..2]);
        store.0.get_mut(&commit_key).unwrap()[10] ^= 1;
        assert_eq!(
            Reader::load(&store, "fossil-demo/".into(), 8453)
                .await
                .err()
                .unwrap(),
            ArchiveError::Integrity
        );

        let (mut malformed, _) = fixture();
        malformed
            .0
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
fn active_checkpoint_fallback_is_explicitly_unavailable() {
    block_on(async {
        let store = checkpoint_fixture();
        let mut reader = Reader::load(&store, "fossil-demo/".into(), 8453)
            .await
            .unwrap();
        let absent = parse_address(&Value::String(
            "0x3333333333333333333333333333333333333333".into(),
        ))
        .unwrap();
        assert!(matches!(
            reader.account_at(absent, 102).await,
            Err(ArchiveError::Unavailable(message)) if message.contains("checkpoint fallback")
        ));
    });
}
