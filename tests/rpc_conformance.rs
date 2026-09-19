mod common;

use common::{anchor_package, finalized, hash, ADDRESS, SLOT_A, VALUE_A};
use fossil::archive::{publish, PublicationGate};
use fossil::format::Hash32;
use fossil::normalized::read_package;
use fossil::rpc::{handle_rpc, Publication};
use fossil::store::open_store;
use serde_json::{json, Value};

async fn call(publication: &Publication, method: &str, params: Value) -> Value {
    handle_rpc(
        publication,
        json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
        100,
    )
    .await
}

#[tokio::test]
async fn serves_supported_historical_rpc_and_explicit_errors() {
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
    let cache = tempfile::tempdir().unwrap();
    let publication = Publication::load(store, 1, cache.path().to_path_buf(), 8)
        .await
        .unwrap();

    assert_eq!(
        call(&publication, "eth_getBalance", json!([ADDRESS, "0xa"])).await["result"],
        "0x64"
    );
    assert_eq!(
        call(
            &publication,
            "eth_getTransactionCount",
            json!([ADDRESS, {"blockHash":hash(12),"requireCanonical":true}]),
        )
        .await["result"],
        "0x2"
    );
    assert_eq!(
        call(&publication, "eth_getCode", json!([ADDRESS, "0xa"])).await["result"],
        "0x6000"
    );
    assert_eq!(
        call(
            &publication,
            "eth_getStorageAt",
            json!([ADDRESS, SLOT_A, "0xa"]),
        )
        .await["result"],
        VALUE_A
    );
    assert_eq!(
        call(
            &publication,
            "eth_getStorageAt",
            json!([ADDRESS, SLOT_A, "latest"])
        )
        .await["result"],
        hash(0)
    );
    assert_eq!(
        call(&publication, "eth_blockNumber", json!([])).await["result"],
        "0xc"
    );
    assert_eq!(
        call(&publication, "eth_chainId", json!([])).await["result"],
        "0x1"
    );

    assert_eq!(
        call(&publication, "eth_getBalance", json!([ADDRESS, "pending"])).await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(&publication, "eth_getBalance", json!([ADDRESS, "0x9"])).await["error"]["code"],
        -32001
    );
    assert_eq!(
        call(&publication, "eth_getProof", json!([])).await["error"]["code"],
        -32601
    );
    let batch = handle_rpc(
        &publication,
        json!([
            {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
            {"jsonrpc":"2.0","id":2,"method":"web3_clientVersion","params":[]}
        ]),
        2,
    )
    .await;
    assert_eq!(batch.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn fixed_offset_rejects_safe_and_finalized_tags() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(root.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    publish(
        store.clone(),
        read_package(&anchor_package()).unwrap(),
        PublicationGate::FixedOffset {
            observed_number: 12,
            observed_hash: Hash32([12; 32]),
            offset: 0,
        },
    )
    .await
    .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let publication = Publication::load(store, 1, cache.path().to_path_buf(), 8)
        .await
        .unwrap();

    assert_eq!(
        call(&publication, "eth_getBalance", json!([ADDRESS, "latest"])).await["result"],
        "0xc8"
    );
    for tag in ["safe", "finalized"] {
        let response = call(&publication, "eth_getBalance", json!([ADDRESS, tag])).await;
        assert_eq!(response["error"]["code"], -32001);
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("fixed-offset"));
    }
}
