mod common;

use common::{anchor_package, finalized, ADDRESS, SLOT_A, SLOT_B, VALUE_A, VALUE_B};
use fossil::archive::publish;
use fossil::format::{Address, Hash32};
use fossil::normalized::read_package;
use fossil::rpc::Publication;
use fossil::store::open_store;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn deterministic_history_tombstones_and_cache_reuse() {
    let first_store_dir = tempfile::tempdir().unwrap();
    let second_store_dir = tempfile::tempdir().unwrap();
    let first_store = open_store(first_store_dir.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let second_store = open_store(second_store_dir.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let bytes = anchor_package();
    let first = publish(
        first_store.clone(),
        read_package(&bytes).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    let second = publish(second_store, read_package(&bytes).unwrap(), finalized(12))
        .await
        .unwrap();
    assert_eq!(
        first.manifest, second.manifest,
        "build must be deterministic"
    );

    let retry = publish(
        first_store.clone(),
        read_package(&bytes).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    assert!(retry.idempotent);

    let cache_dir = tempfile::tempdir().unwrap();
    let publication = Arc::new(
        Publication::load(first_store.clone(), 1, cache_dir.path().to_path_buf(), 16)
            .await
            .unwrap(),
    );
    let address = Address::from_str(ADDRESS).unwrap();
    let slot_a = Hash32::from_str(SLOT_A).unwrap();
    let slot_b = Hash32::from_str(SLOT_B).unwrap();

    let at_ten = publication.account_at(address, 10).await.unwrap().unwrap();
    assert_eq!(at_ten.nonce, 1);
    assert_eq!(
        publication.storage_at(address, slot_a, 10).await.unwrap(),
        Hash32::from_str(VALUE_A).unwrap().0
    );
    assert_eq!(
        publication.code_at(address, 10).await.unwrap(),
        [0x60, 0x00]
    );

    assert!(publication.account_at(address, 11).await.unwrap().is_none());
    assert_eq!(
        publication.storage_at(address, slot_a, 11).await.unwrap(),
        [0; 32]
    );

    let recreated = publication.account_at(address, 12).await.unwrap().unwrap();
    assert_eq!(recreated.incarnation, 1);
    assert_eq!(
        publication.storage_at(address, slot_a, 12).await.unwrap(),
        [0; 32]
    );
    assert_eq!(
        publication.storage_at(address, slot_b, 12).await.unwrap(),
        Hash32::from_str(VALUE_B).unwrap().0
    );
    assert!(publication.code_at(address, 12).await.unwrap().is_empty());

    let before = publication.metrics();
    publication.account_at(address, 10).await.unwrap();
    let after = publication.metrics();
    assert_ne!(
        before, after,
        "warm lookup must be reflected in cache metrics"
    );
    assert!(after.contains("fossil_cache_memory_hits_total"));
    assert!(publication
        .account_at(address, 9)
        .await
        .unwrap_err()
        .to_string()
        .contains("block unavailable"));

    drop(publication);
    let restarted = Publication::load(first_store, 1, cache_dir.path().to_path_buf(), 16)
        .await
        .unwrap();
    restarted.account_at(address, 10).await.unwrap();
    assert!(restarted
        .metrics()
        .contains("fossil_cache_disk_hits_total 1"));
}
