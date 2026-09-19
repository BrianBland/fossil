# Fossil

Fossil is an immutable historical-state archive for EVM-compatible chains. **Hot node
disks must provide low-latency, high-throughput I/O for sync and execution; finalized
immutable history does not.** Fossil moves old history to cheap centralized R2/S3
storage so execution nodes can prune or thin and many disposable RPC readers can
scale horizontally over one billed-per-GB backend.

V1 remains readable and tested but frozen. V2 is an incompatible sealed-epoch format,
selected explicitly with `--archive-format v2`; it never falls back to v1 or the
rejected, uncommitted state-key COW prototype.

## V2 sealed epochs

- Writers buffer and seal append-only epochs of at most 1,000 contiguous blocks. An
  open epoch is never visible.
- `SHA256(namespace || full logical key)` fingerprints each changed key; the top seven
  address/code-hash bits route through 128 compact index-delta partitions into 32
  Zstd-9 data objects.
- Index entries contain a 96-bit fingerprint and run pointer. Readers always verify
  the complete key in the data object, so collisions only add candidate work.
- One bounded active directory references a base checkpoint and at most 64 epochs.
  After epoch 64, one partition-local, 8-way-subsharded exact-state checkpoint and a
  two-level numeric completed-window catalog start the next window.
- Account tombstones, explicit zero storage, incarnations, trimmed U256 encoding, and
  separate deduplicated code objects retain existing semantics.
- Historical balance, nonce, code, storage, chain ID, block number, and numeric/tag
  selectors share the RPC response layer. V2 block-hash selectors are explicitly
  unsupported until a durable bounded hash-to-number index exists.

A fixed-size mutable epoch head references one fixed-size immutable commit. Startup is
exactly a head GET and commit GET; directories, index deltas, checkpoint partitions,
and data are fetched and verified lazily. A bounded in-process cache is disposable
and non-authoritative. Every object length and SHA-256 is verified before parsing.

```text
normalized export -> validate/gate -> seal key-major epoch data + index deltas
                                     -> rewrite one active directory
                                     -> fixed-size immutable commit
                                     -> conditional epoch head (last)

query -> select 64k window -> one router partition -> newest applicable epoch deltas
      -> exact full-key verification -> base checkpoint pointer/default
```

The audit parent is never traversed by startup or query. Refresh ignores equal heads,
rejects older/unrelated heads, and may adopt any strictly newer verified authoritative
head with matching chain/genesis/anchor even when polling skipped generations.

See [format](docs/format.md), [semantics](docs/semantics.md),
[consistency](docs/consistency.md), [economics](docs/economics.md),
[Base provenance](docs/base-measurement-provenance.md), [logs design](docs/logs.md),
[historical call design](docs/historical-eth-call.md), and
[limitations](docs/limitations.md).

## Quickstart

```bash
cargo build --release
cargo test

STORE=$(mktemp -d)
cargo run -- archive --archive-format v2 \
  --input tests/fixtures/minimal.jsonl --store "$STORE" --chain-id 0x1 \
  --gate finalized \
  --finalized-head 0:0x0000000000000000000000000000000000000000000000000000000000000010

cargo run -- serve --archive-format v2 --store "$STORE" --chain-id 0x1 \
  --listen 127.0.0.1:8545 --refresh-seconds 30
cargo run -- verify --archive-format v2 --store "$STORE" --chain-id 0x1
```

`verify` checks only the v2 head/commit startup path; lazy objects verify on access.
There is no full-history audit command. V1 rejects `--refresh-seconds`.

Production exporters should submit complete sealed 1,000-block packages. Smaller
sealed packages remain accepted for fixtures and tail/checkpoint operation. State at
block N means post-execution state after N. Missing/deleted accounts and absent
storage return Ethereum zero values.

## Benchmark

The checked artifact is `benchmarks/results/v2-base-shaped-epoch.json`:

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/v2-base-shaped-epoch.json
```

It reports a Base-shaped 1,000-block epoch's data/index/directory/commit bytes and
objects, predecessor and forced-collision correctness, startup GETs, cache-warming
sequence GET/bytes, repeated locally ingested behavior, and a lightweight 64-epoch
closing-checkpoint rollover. The external real 100,000-block
codec result remains layout-only evidence.

Manual 100,000-block mode processes and drops one input chunk at a time, writes a
progress artifact after every epoch, and requires an explicit non-`/tmp` scratch
location. Canonical system `/tmp` descendants are rejected, at least 8 GiB free is
required, progress writes are atomic, and every epoch row records elapsed time,
currently available bytes, and available inodes when `df -Pi` is parseable:

```bash
cargo run --release -- benchmark \
  --manual-blocks 100000 --chunk-blocks 1000 \
  --scratch-dir /mnt/md0/fossil-epoch-bench \
  --output benchmarks/results/manual-v2-100000.json
```

A read-only Base `StaticFileProvider` HyperLogLog pass over the real 100k range
estimated 940,971 unique accounts and 17,339,777 unique `(address, slot)` keys. The
corrected generator preserves 20,614/244,196 local keys per 1,000-block epoch and
slides those pools to the measured global estimates. HLL is approximate; summary
SHA-256 is `e00f51a7cc9be49a2560342c20df44f15bc8dd2a53285eab1bb84042f2fccd8a`.

The corrected Base-cardinality-shaped 100,000-block run completed all 100 sealed epochs
in **228.786 seconds**, producing **17,456 immutable objects** and **1,141,404,991
bytes**. It passed the 10-minute, 35,000-object, and 2.75 GB prototype gates. The
compact result is checked in at
`benchmarks/results/v2-base-shaped-epoch-100k.json`; its full progress result has
SHA-256 `a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.
This remains synthetic cardinality-shaped evidence, not authoritative forward EVM state
or production R2 performance.

Earlier high-cardinality and 256/64/16 fanout runs are retained as rejected tuning
evidence in the provenance document.

The former immutable state-key COW design is rejected: its exact 100k attempt reached
929,134 files (about 3 GB logical/5 GB allocated) and exhausted 1,048,576 `/tmp`
inodes; an md0 rerun accumulated 26 GB and remained unfinished after 15 minutes. This
is retained as rejected-design evidence, not a v2 result.

## Storage and retention

R2 Standard is the recommended interactive backend; S3-compatible storage is the
portable alternative. Workers remain a future stateless serving target. Durable
Objects are unnecessary for bulk storage/read service. No Worker deployment, log
serving, historical EVM execution, distributed routing, or automatic GC is included.

State/data epochs, required deltas/checkpoints/window directories/catalog pages, and
code are durable. Publication never deletes. Superseded active directories, commits,
and unreachable interrupted-write objects may be reclaimed only by a future offline
provider-inventory process after a rollback window; that tooling is not implemented.

Input remains canonical `fossil-export/1`. The first package is exhaustive and later
packages are contiguous authoritative post-state deltas. Exported/checkpoint state is
trusted authoritative input, but Fossil verifies cross-object references, full keys,
checkpoint boundaries, and pointer version time consistency. SHA-256 proves object
integrity, not publisher authenticity, consensus, or Ethereum state-root correctness.
Finality comes from the trusted checkpoint source. Code objects have a 1 MiB prototype
hard limit.

Licensed under MIT.
