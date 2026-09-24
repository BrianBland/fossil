use fossil::format::Hash32;
use fossil::rpc::handle_rpc_with;
use fossil::store::open_store;
use fossil::tiered::Reader;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::process::Command;

const ADDRESS: &str = "0x1111111111111111111111111111111111111111";
const SLOT_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SLOT_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const VALUE_A: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const VALUE_B: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

fn hash(byte: u8) -> String {
    format!("0x{}", hex::encode([byte; 32]))
}

/// Block-zero anchor: create at 0, delete at 1, recreate without code at 2.
fn anchor() -> Vec<u8> {
    let code_hash = Hash32(Keccak256::digest([0x60, 0x00]).into()).to_string();
    let empty = Hash32(Keccak256::digest([]).into()).to_string();
    let zero = hash(0);
    [
        format!(r#"{{"type":"header","schema":"fossil-export/1","chain_id":"0x1","genesis_hash":"{g}","mode":"anchor","anchor_number":"0x0","anchor_hash":"{g}"}}"#, g = hash(1)),
        format!(r#"{{"type":"block","number":"0x0","hash":"{}","parent_hash":"{zero}","state_root":"{}","timestamp":"0x64"}}"#, hash(1), hash(100)),
        format!(r#"{{"type":"account","block":"0x0","address":"{ADDRESS}","exists":true,"incarnation":"0x0","nonce":"0x1","balance":"0x64","code_hash":"{code_hash}"}}"#),
        format!(r#"{{"type":"storage","block":"0x0","address":"{ADDRESS}","incarnation":"0x0","slot":"{SLOT_A}","value":"{VALUE_A}"}}"#),
        format!(r#"{{"type":"code","code_hash":"{code_hash}","bytes":"0x6000"}}"#),
        r#"{"type":"block_end","number":"0x0","event_count":"0x3"}"#.into(),
        format!(r#"{{"type":"block","number":"0x1","hash":"{}","parent_hash":"{}","state_root":"{}","timestamp":"0x65"}}"#, hash(2), hash(1), hash(101)),
        format!(r#"{{"type":"account","block":"0x1","address":"{ADDRESS}","exists":false,"incarnation":"0x0"}}"#),
        format!(r#"{{"type":"storage","block":"0x1","address":"{ADDRESS}","incarnation":"0x0","slot":"{SLOT_A}","value":"{zero}"}}"#),
        r#"{"type":"block_end","number":"0x1","event_count":"0x2"}"#.into(),
        format!(r#"{{"type":"block","number":"0x2","hash":"{}","parent_hash":"{}","state_root":"{}","timestamp":"0x66"}}"#, hash(3), hash(2), hash(102)),
        format!(r#"{{"type":"account","block":"0x2","address":"{ADDRESS}","exists":true,"incarnation":"0x1","nonce":"0x2","balance":"0xc8","code_hash":"{empty}"}}"#),
        format!(r#"{{"type":"storage","block":"0x2","address":"{ADDRESS}","incarnation":"0x1","slot":"{SLOT_B}","value":"{VALUE_B}"}}"#),
        r#"{"type":"block_end","number":"0x2","event_count":"0x2"}"#.into(),
        format!(r#"{{"type":"trailer","end_number":"0x2","end_hash":"{}","block_count":"0x3"}}"#, hash(3)),
    ]
    .join("\n")
    .into_bytes()
}

async fn call(reader: &Reader<'static>, method: &str, params: Value) -> Value {
    let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
    handle_rpc_with(reader, request, 100).await["result"].clone()
}

#[tokio::test]
async fn cli_publishes_compacts_verifies_and_serves_tiered_state() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("anchor.jsonl");
    std::fs::write(&input, anchor()).unwrap();
    let store_path = root.path().join("store");
    let fossil = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_fossil"))
            .args(args)
            .args(["--store", store_path.to_str().unwrap(), "--chain-id", "0x1"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    let gate = [
        "--gate",
        "finalized",
        "--finalized-head",
        &format!("2:{}", hash(3)),
    ];
    let archive = [
        &["archive", "--input", input.to_str().unwrap()][..],
        &gate[..],
    ]
    .concat();
    assert!(fossil(&archive).contains("published through 2"));
    assert!(fossil(&archive).contains("published through 2"));
    assert!(fossil(&["compact"]).contains("drained after 0 commits"));
    assert!(fossil(&["verify"]).contains("through 2 with 1 runs"));
    let samples = root.path().join("samples.jsonl");
    std::fs::write(
        &samples,
        format!(
            "{{\"address\":\"{ADDRESS}\",\"slot\":\"{SLOT_A}\",\"block\":0}}\n{{\"address\":\"{ADDRESS}\",\"slot\":\"{SLOT_B}\",\"block\":2}}\n"
        ),
    )
    .unwrap();
    let probe: Value =
        serde_json::from_str(&fossil(&["probe", "--input", samples.to_str().unwrap()])).unwrap();
    assert_eq!(probe["samples"], 2);
    assert_eq!(probe["errors"], 0);
    // head + one summary shard + fence/data for the account and the slot.
    assert!(probe["gets_max"].as_u64().unwrap() <= 1 + 1 + 6, "{probe}");

    // A process-lifetime store, as the server uses.
    let store = Box::leak(Box::new(
        open_store(store_path.to_str().unwrap(), None, None)
            .await
            .unwrap(),
    ));
    let store: &'static _ = store;
    let reader = Reader::open(&**store, 1).await.unwrap();
    assert_eq!(
        call(&reader, "eth_getBalance", json!([ADDRESS, "0x0"])).await,
        "0x64"
    );
    assert_eq!(
        call(&reader, "eth_getCode", json!([ADDRESS, "0x0"])).await,
        "0x6000"
    );
    assert_eq!(
        call(&reader, "eth_getStorageAt", json!([ADDRESS, SLOT_A, "0x0"])).await,
        VALUE_A
    );
    assert_eq!(
        call(&reader, "eth_getBalance", json!([ADDRESS, "0x1"])).await,
        "0x0"
    );
    assert_eq!(
        call(&reader, "eth_getStorageAt", json!([ADDRESS, SLOT_A, "0x1"])).await,
        hash(0)
    );
    assert_eq!(
        call(
            &reader,
            "eth_getTransactionCount",
            json!([ADDRESS, "latest"])
        )
        .await,
        "0x2"
    );
    assert_eq!(
        call(&reader, "eth_getCode", json!([ADDRESS, "latest"])).await,
        "0x"
    );
    assert_eq!(
        call(&reader, "eth_getStorageAt", json!([ADDRESS, SLOT_A, "0x2"])).await,
        hash(0)
    );
    assert_eq!(
        call(&reader, "eth_getStorageAt", json!([ADDRESS, SLOT_B, "0x2"])).await,
        VALUE_B
    );
    assert_eq!(call(&reader, "eth_blockNumber", json!([])).await, "0x2");
    let beyond = handle_rpc_with(
        &reader,
        json!({"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":[ADDRESS, "0x3"]}),
        100,
    )
    .await;
    assert!(beyond.get("error").is_some(), "{beyond}");
}
