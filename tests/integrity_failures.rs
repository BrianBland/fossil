mod common;

use common::{anchor_package, finalized};
use fossil::archive::{load_head_manifest, publish};
use fossil::normalized::read_package;
use fossil::rpc::Publication;
use fossil::store::open_store;

#[tokio::test]
async fn corrupted_manifest_and_index_fail_before_readiness() {
    let manifest_root = tempfile::tempdir().unwrap();
    let manifest_store = open_store(manifest_root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let outcome = publish(
        manifest_store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    std::fs::write(
        manifest_root.path().join(outcome.manifest.object_key()),
        b"corrupt",
    )
    .unwrap();
    let error = load_head_manifest(manifest_store.as_ref(), 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("manifest digest mismatch"));

    let index_root = tempfile::tempdir().unwrap();
    let index_store = open_store(index_root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    publish(
        index_store.clone(),
        read_package(&anchor_package()).unwrap(),
        finalized(12),
    )
    .await
    .unwrap();
    let (_, _, manifest) = load_head_manifest(index_store.as_ref(), 1).await.unwrap();
    std::fs::write(
        index_root
            .path()
            .join(manifest.segments[0].index_sha256.object_key()),
        b"corrupt",
    )
    .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let error = Publication::load(index_store, 1, cache.path().to_path_buf(), 8)
        .await
        .err()
        .expect("corrupt index must prevent readiness");
    assert!(error
        .to_string()
        .contains("index length or digest mismatch"));
}
