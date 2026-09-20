# Fossil

Fossil is an immutable historical-state archive for EVM-compatible chains. Hot nodes
need low-latency disks for sync and execution; finalized history does not. Fossil lets
a hot or pruned node continuously export sealed history into one centralized
R2/S3-compatible object store, while many cheap, disposable readers share that store.

This repository defines **Fossil on-disk format version 1**: append-only sealed epochs,
a single mutable head at `chains/<chain-id>/heads/finalized.bin`, immutable
content-addressed objects, and head-last atomic publication. There is one archive
format and one CLI path.

Start with the illustrated [architecture and storage walkthrough](docs/architecture.md).
Try the public, read-only [live Base R2/Cloudflare Worker demo](docs/live-demo.md).
Runtime choices for the native server and the bounded Rust/WASM Cloudflare archive
reader are described in [deployment](docs/deployment.md). Worker setup and scope are
in [`worker/README.md`](worker/README.md). See also [format](docs/format.md),
[content integrity](docs/integrity.md), [semantics](docs/semantics.md),
[consistency](docs/consistency.md), [economics](docs/economics.md),
[Base measurement provenance](docs/base-measurement-provenance.md),
[logs design](docs/logs.md), [historical call design](docs/historical-eth-call.md), and
[limitations](docs/limitations.md).

## Sealed epochs

- Writers seal at most 1,000 contiguous blocks. An open epoch is never visible.
- The top seven address/code-hash route bits select one of 128 compact index-delta
  partitions; four router partitions share each of 32 Zstd-9 data objects.
- A 96-bit fingerprint identifies candidates, but readers always verify the complete
  key in the data object. Collisions add work, never false values.
- One active directory references a base checkpoint and at most 64 sealed epoch
  descriptors, each covering at most 1,000 blocks. At rollover, the completed
  directory enters the bounded two-level catalog; its closing exact-state checkpoint
  becomes the base checkpoint of a new empty active directory.
- Account tombstones, explicit zero storage, incarnations, trimmed U256 encoding, and
  deduplicated code objects preserve Ethereum state semantics.
- Startup is one bounded head GET and one immutable commit GET. Directories, indexes,
  checkpoints, and data load lazily into a bounded disposable in-process cache.
- Publication uploads immutable objects and a fixed-size commit first, then changes
  the head with compare-and-swap. Readers see either the old complete publication or
  the new complete publication.

Supported RPC methods include historical balance, nonce, code, storage, chain ID, and
block number with numeric and standard tag selectors. Block-hash selectors are
rejected until a durable bounded hash-to-number index exists.

## Quickstart

```bash
cargo build --release
cargo test

STORE=$(mktemp -d)
cargo run -- archive \
  --input tests/fixtures/minimal.jsonl --store "$STORE" --chain-id 0x1 \
  --gate finalized \
  --finalized-head 0:0x0000000000000000000000000000000000000000000000000000000000000010

cargo run -- serve --store "$STORE" --chain-id 0x1 \
  --listen 127.0.0.1:8545 --refresh-seconds 30
cargo run -- verify --store "$STORE" --chain-id 0x1
```

`verify` checks the bounded head/commit startup path. Every lazy object verifies its
length, SHA-256 digest, magic, version, and structural bounds when accessed; there is
not yet a full-history audit command.

Production exporters should submit complete sealed 1,000-block packages. Smaller
sealed packages remain accepted for fixtures and tail/checkpoint operation. State at
block N means post-execution state after N. Missing/deleted accounts and absent
storage return Ethereum zero values.

## Benchmarks

The live Rust/WASM Worker demo now exposes an exact sparse overlay across Base blocks
`51,232,601..51,535,000` (302,400 blocks/303 epochs). The 81,245,782-byte query
archive built in 294.920 seconds and uploaded to R2 in 71 seconds. A separate
4,427,761,942-byte real-week physical-layout corpus is retained as layout evidence,
not queryable forward state. After both uploads, the bucket remained below its 5 GB
cap at 4,514,343,105 bytes. Results, environment-specific query medians, and scope
caveats are in
[`benchmarks/results/live-week-demo.json`](benchmarks/results/live-week-demo.json)
and the [copy-paste live guide](docs/live-demo.md). This sparse overlay is exact only
for its documented vault, WETH-contract native ETH balance, and pool reserve keys; it
is not arbitrary-address-complete.

The checked 1,000-block artifact is
[`benchmarks/results/base-shaped-epoch.json`](benchmarks/results/base-shaped-epoch.json):

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/base-shaped-epoch.json
```

The completed Base-cardinality-shaped 100,000-block run sealed 100 epochs in
**228.786 seconds**, producing **17,456 immutable objects** and **1,141,404,991
bytes**. Its compact result is
[`benchmarks/results/base-shaped-epoch-100k.json`](benchmarks/results/base-shaped-epoch-100k.json).
This is synthetic cardinality-shaped filesystem evidence, not authoritative forward
EVM state or production R2 performance. It is a retained historical run that predates
the new month-universe storage-address mapping, not a claimed result from the current
generator without rerunning 100k. The full progress result has SHA-256
`a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.

Real read-only `StaticFileProvider` measurement now covers Base blocks
50,229,111..51,525,110 (1,296,000 blocks). Across 277,758,377 account rows and
1,154,636,022 storage rows, HLL estimated 7,956,411 unique accounts and 218,828,772
unique `(address, slot)` keys (summary
`42c965456b49243378b4c106c99ee6be50a40611adff5bf8232e1ab3bf5b511e`). Reth
changeset data plus offsets occupied 93,831,464,168 bytes (summary
`800390687bcea3ebc1b24dc50589b1708d246af5550b23a561234b4a1b5a1011`).

A real month **layout-only** pass over 1,296 1,000-block chunks took 814.453 seconds
summed across five resumable slices. Block-major Zstd-9 used 24,476,613,098 bytes;
key-major used 20,030,070,751 bytes (15,455.30 bytes/block, 18.1665% smaller; summary
`15fb23b5244a74c3896cfc2cf6aa51a06e1470d0f28b11daf286981cbea2434c`). These
measurements do not publish or serve a Fossil archive.

Manual mode accepts a nonzero block count divisible by the chunk size, with
`--chunk-blocks` in `1..=1000`. The completed deterministic 30-day stress test used
1,296,000 blocks and 1,000-block chunks. It sealed all 1,296 epochs in **9,449.0865
seconds (2h37m29s)**, producing 20 completed windows plus 16 active epochs,
**234,368 immutable objects**, and **23,277,861,710 logical immutable bytes**. Those
objects include 41,472 data, 165,888 index, 23,060 checkpoint, 1,316 directory, 40
catalog, and 1,296 commit objects. The compact checked result is
[`benchmarks/results/base-shaped-month.json`](benchmarks/results/base-shaped-month.json);
the full progress result has SHA-256
`c72c2960789cbf6000c42624dacaab03d70939a239747149dc3825d901c977c4`.

This is a filesystem-backed, synthetic Base-cardinality-shaped stress test. It is not
production R2 performance or billing evidence, and it does not contain authoritative,
semantically complete forward EVM state, code traffic, or a production exhaustive
anchor. Manual mode requires an explicit non-`/tmp` scratch directory with at least 8
GiB free, starts from an empty store, drops each input chunk after sealing, and
atomically writes progress after every epoch. The deterministic pool formulas are
tested across all 1,296 epochs, cap IDs at measured global pools, and exercise
checkpoint/catalog rollover.

## Storage, trust, and scope

R2 Standard is the recommended interactive backend; an S3-compatible service is the
portable alternative. The product architecture is one centralized immutable backend,
not a DHT or federation. State epochs, indexes, checkpoints, directories, catalogs,
and code are durable. Publication never deletes. Offline inventory and garbage
collection are future work.

The native Tokio/Axum binary is the full JSON-RPC server. The Rust/WASM Cloudflare
Worker is a bounded catalog/checkpoint reader and read-only object gateway, not a
second archive. A sparse public [live demo](docs/live-demo.md) serves standard balance,
nonce, code, storage, chain ID, block-number, and client-version methods directly from
R2. Its week overlay is exact for the explicitly tracked keys, including the native ETH
balance held by the WETH contract, but is not arbitrary-address-complete. Logs and
historical EVM execution are not implemented.

Input remains canonical `fossil-export/1`. The exporter/finality source is trusted.
Fossil validates continuity, references, bounds, and content integrity, but SHA-256
does not prove publisher authenticity, consensus, or the Ethereum state root.

Licensed under MIT.
