# Fossil

Fossil is an immutable historical-state archive for EVM-compatible chains. Hot nodes
need low-latency disks for sync and execution; finalized history does not. Fossil lets
a node continuously export sealed history into one R2/S3-compatible object store, while
many cheap, disposable readers — the native server or a Rust/WASM Cloudflare Worker —
share that store.

New here? Start with the [visual storage guide](docs/storage-guide.md).

This repository defines the **tiered v1** format ([format](docs/format.md)): every
sealed epoch becomes an immutable run of exact `(key, block)` versions; runs are
merged into logarithmically many time-disjoint levels by a separate compactor; and one
mutable JSON head, `chains/<chain-id>/heads/tiered-v1.json`, carries the whole run
manifest inline and is replaced by compare-and-swap only after every object it
references is durable.

- **Reads.** A cold account-plus-storage read is one head GET, one summary GET per run
  that could contain the key (no-false-negative Bloom shards; an address's account and
  storage share one), and at most three fence/data GETs per hit. Every request has hard
  limits of 192 GETs and 128 MiB fetched.
- **Writes.** `archive` appends one package as an L0 run and never compacts;
  `compact` folds four L0 runs at a time through levels with fanout eight. Both commit
  by head CAS and tolerate each other; a 16-run L0 backlog applies backpressure.
- **Semantics.** State at block N is post-execution state; account tombstones, zero
  slots and incarnations are versions. The archive starts from an exhaustive block-zero
  anchor. See [semantics](docs/semantics.md) and [consistency](docs/consistency.md).

The shared [`codec`](codec) crate holds the run, summary, head and key encodings, so the
native reader and the [Worker](worker/README.md) decode identical bytes; a parity test
checks them against each other.

## Quickstart

```bash
cargo build --release
cargo test

STORE=$(mktemp -d)
cargo run -- archive \
  --input tests/fixtures/minimal.jsonl --store "$STORE" --chain-id 0x1 \
  --gate finalized \
  --finalized-head 0:0x0000000000000000000000000000000000000000000000000000000000000010
cargo run -- compact --store "$STORE" --chain-id 0x1
cargo run -- verify --store "$STORE" --chain-id 0x1
cargo run -- serve --store "$STORE" --chain-id 0x1 \
  --listen 127.0.0.1:8545 --refresh-seconds 30
```

`fossil anchor` builds the block-zero package from a genesis allocation. `fossil probe`
measures cold GETs and bytes per account-plus-storage read over sampled queries. The
[Base exporter](tools/base-export/README.md) replays Base from genesis and publishes
through the CLI.

Supported RPC methods: `eth_chainId`, `eth_blockNumber`, `eth_getBalance`,
`eth_getTransactionCount`, `eth_getCode`, `eth_getStorageAt`, and `web3_clientVersion`,
with numeric and tag selectors. Block-hash selectors are rejected. Logs, calls and
tracing are unsupported ([limitations](docs/limitations.md)).

Input is canonical `fossil-export/1`. The exporter and finality source are trusted:
Fossil validates continuity, lifecycle consistency, bounds and content integrity, but
SHA-256 does not prove publisher authenticity or the Ethereum state root.

Licensed under MIT.
