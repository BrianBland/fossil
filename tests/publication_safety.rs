mod common;

use common::{anchor_package, finalized, hash, ADDRESS};
use fossil::archive::{load_head_manifest, publish, PublicationGate};
use fossil::format::{Address, Hash32};
use fossil::normalized::read_package;
use fossil::rpc::Publication;
use fossil::store::open_store;
use sha3::{Digest, Keccak256};
use std::str::FromStr;

#[tokio::test]
async fn rejects_unsafe_gates_and_canonical_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let bytes = anchor_package();

    let wrong_gate = PublicationGate::Finalized {
        number: 12,
        hash: Hash32([99; 32]),
    };
    let error = publish(store.clone(), read_package(&bytes).unwrap(), wrong_gate)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint hash"));
    assert!(!root.path().join("chains/0x1/heads/finalized.json").exists());

    let outcome = publish(
        store.clone(),
        read_package(&bytes).unwrap(),
        PublicationGate::FixedOffset {
            observed_number: 12,
            observed_hash: Hash32([12; 32]),
            offset: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome.published_number, 11);

    let conflict = publish(store, read_package(&bytes).unwrap(), finalized(12))
        .await
        .unwrap_err();
    assert!(conflict.to_string().contains("canonical conflict"));
}

#[tokio::test]
async fn rejects_missing_referenced_bytecode() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let package = String::from_utf8(anchor_package())
        .unwrap()
        .lines()
        .filter(|line| !line.contains("\"type\":\"code\""))
        .collect::<Vec<_>>()
        .join("\n")
        .replacen("\"event_count\":\"0x3\"", "\"event_count\":\"0x2\"", 1);
    let error = publish(
        store,
        read_package(package.as_bytes()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unavailable at block"));
}

#[tokio::test]
async fn rejects_bytecode_first_seen_after_account_event() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let original = String::from_utf8(anchor_package()).unwrap();
    let code = original
        .lines()
        .find(|line| line.contains("\"type\":\"code\""))
        .unwrap();
    let mut lines = Vec::new();
    for line in original.lines() {
        if line == code {
            continue;
        }
        if line.contains("\"block_end\"") && line.contains("\"0xa\"") {
            lines.push(line.replace("\"0x3\"", "\"0x2\""));
        } else if line.contains("\"block_end\"") && line.contains("\"0xc\"") {
            lines.push(code.to_owned());
            lines.push(line.replace("\"0x2\"", "\"0x3\""));
        } else {
            lines.push(line.to_owned());
        }
    }
    let package = lines.join("\n");
    let error = publish(
        store,
        read_package(package.as_bytes()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unavailable at block 10"));
}

#[tokio::test]
async fn prior_generation_bytecode_is_available_to_later_accounts() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    publish(
        store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    let code_hash = Hash32(Keccak256::digest([0x60_u8, 0x00]).into());
    let delta = format!(
        concat!(
            "{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{}\",\"mode\":\"delta\",\"preceding_number\":\"0xc\",\"preceding_hash\":\"{}\"}}\n",
            "{{\"type\":\"block\",\"number\":\"0xd\",\"hash\":\"{}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x67\"}}\n",
            "{{\"type\":\"account\",\"block\":\"0xd\",\"address\":\"{}\",\"exists\":true,\"incarnation\":\"0x1\",\"nonce\":\"0x3\",\"balance\":\"0x12c\",\"code_hash\":\"{}\"}}\n",
            "{{\"type\":\"block_end\",\"number\":\"0xd\",\"event_count\":\"0x1\"}}\n",
            "{{\"type\":\"trailer\",\"end_number\":\"0xd\",\"end_hash\":\"{}\",\"block_count\":\"0x1\"}}\n"
        ),
        hash(1),
        hash(12),
        hash(13),
        hash(12),
        hash(113),
        ADDRESS,
        code_hash,
        hash(13),
    );
    let outcome = publish(
        store,
        read_package(delta.as_bytes()).unwrap(),
        finalized(13),
    )
    .await
    .unwrap();
    assert_eq!(outcome.published_number, 13);
}

#[test]
fn rejects_parent_conflicts_and_noncanonical_quantities() {
    let bytes = anchor_package();
    let wrong_parent = String::from_utf8(bytes.clone())
        .unwrap()
        .replacen(&hash(10), &hash(55), 1);
    let error = read_package(wrong_parent.as_bytes()).unwrap_err();
    assert!(error.to_string().contains("anchor header") || error.to_string().contains("parent"));

    let bad_quantity =
        String::from_utf8(bytes)
            .unwrap()
            .replacen("\"number\":\"0xa\"", "\"number\":\"0x0a\"", 1);
    assert!(read_package(bad_quantity.as_bytes()).is_err());
}

#[tokio::test]
async fn corrupted_segment_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    publish(
        store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    let (_, _, manifest) = load_head_manifest(store.as_ref(), 1).await.unwrap();
    let descriptor = &manifest.segments[0];
    std::fs::write(
        root.path().join(descriptor.segment_sha256.object_key()),
        b"corrupt",
    )
    .unwrap();

    let cache = tempfile::tempdir().unwrap();
    let publication = Publication::load(store, 1, cache.path().to_path_buf(), 8)
        .await
        .unwrap();
    let error = publication
        .account_at(Address::from_str(ADDRESS).unwrap(), 10)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("digest mismatch"));
}
