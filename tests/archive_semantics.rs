mod common;

use common::{anchor_package, finalized, ADDRESS, SLOT_A, SLOT_B};
use fossil::archive::{self, PublicationGate};
use fossil::format::{Address, Hash32};
use fossil::normalized::read_package;
use fossil::rpc::handle_rpc;
use fossil::store::{ArchiveStore, MemoryArchiveStore, VersionedBytes};
use serde_json::json;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use sha3::{Digest, Keccak256};

fn delta_with_reemitted_code() -> Vec<u8> {
    let code_hash = Hash32(Keccak256::digest([0x60_u8, 0x00]).into());
    format!(
        concat!(
            "{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{}\",\"mode\":\"delta\",\"preceding_number\":\"0xc\",\"preceding_hash\":\"{}\"}}\n",
            "{{\"type\":\"block\",\"number\":\"0xd\",\"hash\":\"{}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x67\"}}\n",
            "{{\"type\":\"account\",\"block\":\"0xd\",\"address\":\"{}\",\"exists\":true,\"incarnation\":\"0x1\",\"nonce\":\"0x3\",\"balance\":\"0x12c\",\"code_hash\":\"{}\"}}\n",
            "{{\"type\":\"code\",\"code_hash\":\"{}\",\"bytes\":\"0x6000\"}}\n",
            "{{\"type\":\"block_end\",\"number\":\"0xd\",\"event_count\":\"0x2\"}}\n",
            "{{\"type\":\"trailer\",\"end_number\":\"0xd\",\"end_hash\":\"{}\",\"block_count\":\"0x1\"}}\n"
        ),
        common::hash(1), common::hash(12), common::hash(13), common::hash(12),
        common::hash(113), ADDRESS, code_hash, code_hash, common::hash(13),
    ).into_bytes()
}

#[derive(Default)]
struct CasLoserStore {
    inner: MemoryArchiveStore,
    lose_once: AtomicBool,
}

#[async_trait]
impl ArchiveStore for CasLoserStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get(key).await
    }
    async fn get_bounded(&self, key: &str, maximum: usize) -> Result<Vec<u8>> {
        self.inner.get_bounded(key, maximum).await
    }
    async fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes).await
    }
    async fn read_mutable(&self, key: &str) -> Result<Option<VersionedBytes>> {
        self.inner.read_mutable(key).await
    }
    async fn read_mutable_bounded(
        &self,
        key: &str,
        maximum: usize,
    ) -> Result<Option<VersionedBytes>> {
        self.inner.read_mutable_bounded(key, maximum).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&VersionedBytes>,
        bytes: &[u8],
    ) -> Result<()> {
        self.inner.compare_and_swap(key, expected, bytes).await?;
        if !self.lose_once.swap(true, Ordering::SeqCst) {
            bail!("simulated CAS loser");
        }
        Ok(())
    }
}

#[tokio::test]
async fn rejects_block_hash_selectors_until_bounded_index_exists() {
    let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    archive::publish(
        store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    let publication = archive::Publication::load(store, 1).await.unwrap();
    let response = handle_rpc(
        &publication,
        json!({"jsonrpc":"2.0","id":8,"method":"eth_getBalance","params":[ADDRESS,{"blockHash":common::hash(10),"requireCanonical":true}]}),
        100,
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("bounded durable hash index"));
}

#[tokio::test]
async fn anchor_delta_and_cas_loser_retries_are_idempotent() {
    let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    let anchor = read_package(&anchor_package()).unwrap();
    let first = archive::publish(store.clone(), anchor.clone(), finalized(12))
        .await
        .unwrap();
    let retry = archive::publish(store.clone(), anchor, finalized(12))
        .await
        .unwrap();
    assert!(!first.idempotent);
    assert!(retry.idempotent);
    assert_eq!(retry.commit, first.commit);

    let delta = read_package(&delta_with_reemitted_code()).unwrap();
    let first = archive::publish(store.clone(), delta.clone(), finalized(13))
        .await
        .unwrap();
    let retry = archive::publish(store, delta, finalized(13)).await.unwrap();
    assert!(!first.idempotent);
    assert!(retry.idempotent);
    assert_eq!(retry.commit, first.commit);

    let loser = Arc::new(CasLoserStore::default());
    let package = read_package(&anchor_package()).unwrap();
    assert!(
        archive::publish(loser.clone(), package.clone(), finalized(12))
            .await
            .is_err()
    );
    let retry = archive::publish(loser, package, finalized(12))
        .await
        .unwrap();
    assert!(retry.idempotent);
}

#[tokio::test]
async fn reemitted_code_preserves_old_block_code_metadata() {
    let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    archive::publish(
        store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    archive::publish(
        store.clone(),
        read_package(&delta_with_reemitted_code()).unwrap(),
        finalized(13),
    )
    .await
    .unwrap();
    let publication = archive::Publication::load(store, 1).await.unwrap();
    let code = publication
        .code_at(Address::from_str(ADDRESS).unwrap(), 10)
        .await
        .unwrap();
    assert_eq!(code, [0x60, 0x00]);
}

#[tokio::test]
async fn fixed_offset_publishes_eligible_prefix_and_rejects_finalized_selector() {
    let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    let package = read_package(&anchor_package()).unwrap();
    archive::publish(
        store.clone(),
        package,
        PublicationGate::FixedOffset {
            observed_number: 12,
            observed_hash: Hash32([12; 32]),
            offset: 2,
        },
    )
    .await
    .unwrap();
    let publication = archive::Publication::load(store, 1).await.unwrap();
    assert_eq!(publication.commit.published_number, 10);
    let response = handle_rpc(
        &publication,
        json!({"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":[ADDRESS,"finalized"]}),
        100,
    )
    .await;
    assert_eq!(response["error"]["code"], -32001);
}

#[tokio::test]
async fn preserves_tombstones_zeroes_and_incarnations() {
    let store: Arc<dyn ArchiveStore> = Arc::new(MemoryArchiveStore::default());
    let package = read_package(&anchor_package()).unwrap();
    archive::publish(store.clone(), package, finalized(12))
        .await
        .unwrap();
    let publication = archive::Publication::load(store, 1).await.unwrap();
    let address = Address::from_str(ADDRESS).unwrap();
    assert!(publication.account_at(address, 11).await.unwrap().is_none());
    assert_eq!(
        publication
            .storage_at(address, Hash32::from_str(SLOT_A).unwrap(), 11)
            .await
            .unwrap(),
        [0; 32]
    );
    assert_eq!(
        publication
            .storage_at(address, Hash32::from_str(SLOT_A).unwrap(), 12)
            .await
            .unwrap(),
        [0; 32]
    );
    assert_ne!(
        publication
            .storage_at(address, Hash32::from_str(SLOT_B).unwrap(), 12)
            .await
            .unwrap(),
        [0; 32]
    );
}
