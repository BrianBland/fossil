use fossil::store::{open_store, ArchiveStore};
use std::sync::Arc;

async fn assert_store_contract(store: Arc<dyn ArchiveStore>) {
    store.put_immutable("objects/value", b"one").await.unwrap();
    store.put_immutable("objects/value", b"one").await.unwrap();
    assert_eq!(store.get("objects/value").await.unwrap(), b"one");
    assert!(store
        .put_immutable("objects/value", b"collision")
        .await
        .is_err());

    store
        .compare_and_swap("head", None, b"first")
        .await
        .unwrap();
    assert!(store
        .compare_and_swap("head", None, b"again")
        .await
        .is_err());
    let first = store.read_mutable("head").await.unwrap().unwrap();
    store
        .compare_and_swap("head", Some(&first), b"second")
        .await
        .unwrap();
    assert!(store
        .compare_and_swap("head", Some(&first), b"stale")
        .await
        .is_err());
    assert_eq!(
        store.read_mutable("head").await.unwrap().unwrap().bytes,
        b"second"
    );
}

#[tokio::test]
async fn memory_backend_contract() {
    let store = open_store("memory://", None, None).await.unwrap();
    assert_store_contract(store).await;
}

#[tokio::test]
async fn filesystem_backend_contract_and_reopen() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("new").join("nested").join("archive");
    let uri = root.to_str().unwrap();
    assert!(!root.exists());
    let store = open_store(uri, None, None).await.unwrap();
    assert_store_contract(store).await;

    // Reopening exercises the observable side of the fsync/rename path: both the
    // immutable ancestor tree and visible mutable head are complete.
    let reopened = open_store(uri, None, None).await.unwrap();
    assert_eq!(reopened.get("objects/value").await.unwrap(), b"one");
    assert_eq!(
        reopened.read_mutable("head").await.unwrap().unwrap().bytes,
        b"second"
    );
}
