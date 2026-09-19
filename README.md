# Fossil

Fossil is a standalone Rust prototype for publishing and serving **bootstrap-bounded,
immutable historical Ethereum post-state** from normalized exports. It is aimed at
Base/Reth experiments, but it is not an archive-node replacement and does not yet
read Reth databases directly.

The durable source of truth is a filesystem directory or S3-compatible bucket.
State segments, indexes, and manifests are immutable and addressed by SHA-256. A
small mutable chain head is replaced last with a conditional write, giving readers
atomic visibility of a complete publication (not a multi-object transaction).

## What works

- inspectable `fossil-export/1` JSONL input (plain or zstd compressed);
- complete anchor followed by contiguous delta packages;
- post-block account, storage, code, and canonical block records;
- explicit account tombstones, explicit zero storage, and account incarnations;
- finalized-checkpoint and fixed-offset publication gates;
- deterministic zstd segments, sorted sidecar indexes, SHA-256 integrity checks,
  create-only immutable uploads, and conditional head publication;
- filesystem and `object_store` S3 backends (including custom endpoints);
- eager verified index mirroring, bounded weighted memory cache, verified persistent
  segment cache, and singleflight cache fills;
- historical `eth_getBalance`, `eth_getTransactionCount`, `eth_getCode`, and
  `eth_getStorageAt`, plus `eth_chainId`, `eth_blockNumber`, and
  `web3_clientVersion`;
- numeric, `earliest` (genesis anchors only), `latest`, and EIP-1898 number/hash
  selectors. `safe`/`finalized` resolve only for finalized publications; fixed-offset
  publications reject them. `pending` is rejected;
- JSON-RPC batches (default maximum 100), `/health`, and Prometheus text at
  `/metrics`.

State at block N means post-execution state after N. Queries before the anchor or
after the published head fail with `-32001 block unavailable`. Missing/deleted
accounts return Ethereum zero values. Storage is looked up using the account
incarnation active at the requested block, so deleted contract storage cannot leak
into a recreated contract.

## Architecture

```text
normalized export -> validation/gate -> deterministic zstd segment
                                      -> sorted immutable index
                                      -> complete immutable manifest
                                      -> conditional mutable head (last)

head -> verified manifest -> eagerly mirrored indexes -> memory/disk segment cache
                                                    -> historical state fold -> RPC
```

Objects are stored as `objects/sha256/<first-two>/<digest>`. The only mutable key is
`chains/<chain-id>/heads/finalized.json`. A generation manifest contains the full
segment catalog and points to its parent manifest. Readers never use bucket listing.
An interrupted writer can leave unreachable content-addressed objects, but cannot
make them visible. Filesystem publication uses an exclusive lock, fsync, and atomic
rename. S3 uses provider conditional create/update requests and fails rather than
falling back to last-writer-wins behavior.

See [format](docs/format.md), [semantics](docs/semantics.md),
[consistency](docs/consistency.md), [economics](docs/economics.md),
[federated archive roadmap](docs/federation.md), and
[limitations](docs/limitations.md).

### Future federation seam

Content IDs are independent of location and archive code depends on a narrow storage
trait. A future Portal-Network-like backend could let peers retain full or fractional
object sets backed locally by memory, disk, Redis, or S3 while verifying every object
against the same manifest hashes. This prototype does **not** implement a DHT,
networking, discovery, replication, incentives, or availability guarantees. Metrics
report total segment count and immutable segment/index bytes for later fractional
capacity simulations.

## Quickstart

Build and test:

```bash
cargo build --release
cargo test
```

Reproduce the fixed-seed local headline benchmark and overwrite the checked result:

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/2026-09-19-local.json
```

The JSON includes workload shape, sizes/compression, cold and warm percentiles,
throughput at concurrency one, counted backend reads and amplification, cache ratio,
archive size, build/platform metadata, and dated illustrative cost formulas. It is a
local prototype measurement, not a production claim.

Publish the fixture using an explicit finalized checkpoint (the fixture is a format
example; integration tests generate a richer chain):

```bash
STORE=$(mktemp -d)
cargo run -- archive \
  --input tests/fixtures/minimal.jsonl \
  --store "$STORE" \
  --chain-id 0x1 \
  --gate finalized \
  --finalized-head 0:0x0000000000000000000000000000000000000000000000000000000000000010

cargo run -- serve \
  --store "$STORE" --chain-id 0x1 \
  --cache-dir "$STORE/cache" --listen 127.0.0.1:8545
```

In another shell:

```bash
curl -s localhost:8545 -H 'content-type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","latest"]}'
curl -s localhost:8545/health
curl -s localhost:8545/metrics
cargo run -- verify --store "$STORE" --chain-id 0x1
```

A fixed-offset development policy requires the observed tip to be present in the
input and publishes only through `tip - offset`:

```bash
fossil archive --input export.jsonl.zst --store /srv/fossil --chain-id 0x2105 \
  --gate fixed-offset --observed-tip 50001000:0x... --offset 1000
```

Fixed offset is explicitly **not** protocol finality.

## S3-compatible storage

`object_store` 0.11 supports direct `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / optional `AWS_SESSION_TOKEN`, web identity
(`AWS_WEB_IDENTITY_TOKEN_FILE` plus `AWS_ROLE_ARN`), ECS container credentials, and
EC2 instance credentials. It does **not** load `AWS_PROFILE`. Secrets are never CLI
arguments.

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
fossil archive --input export.jsonl.zst --store s3://base-fossil/prod \
  --chain-id 0x2105 --gate finalized --finalized-head 50000000:0x...

fossil serve --store s3://base-fossil/prod --chain-id 0x2105 \
  --s3-endpoint https://ACCOUNT.r2.cloudflarestorage.com \
  --s3-region auto --cache-dir /var/lib/fossil
```

For a read-heavy archive RPC, Cloudflare **R2 Standard** is generally a better fit
than infrequent-access/archive tiers because request and restore costs dominate and
R2 has no Internet egress fee. Use **S3 Glacier Instant Retrieval** only for genuinely
cold, large objects with a measured low GET rate; its retrieval and minimum-duration
charges are a poor default for interactive RPC. Keep indexes and hot segments on
local SSD. Validate conditional create/CAS behavior against the exact AWS/R2 account
and client version before production use; CI proves memory/filesystem contracts and
the generic conditional object-store adapter, but real AWS and R2 smoke tests remain
required.

## Normalized exporter contract

The first package is an exhaustive snapshot at genesis or an explicit anchor:
every live account, every nonzero slot, and all referenced bytecode. Later packages
are deltas containing authoritative final post-state. Blocks are contiguous and
parent-linked. Account records are address-sorted per block; storage records are
sorted by `(address, incarnation, slot)`. Hex is lowercase and quantities are
canonical. Code bytes must match Ethereum Keccak-256 `code_hash`.

Raw Reth storage-v2 changesets are insufficient: they do not alone provide a complete
forward post-state stream, an initial anchor, bytecode, indexes, or safe
creation/deletion incarnation semantics. A Base/Reth exporter is a follow-up, not a
claim of this prototype.

### ExEx integration design

A version-pinned Reth ExEx (or dedicated exporter) should:

1. obtain canonical block identity and final post-block state from tested Reth APIs;
2. build one exhaustive anchor by walking hashed/plain account state, nonzero
   storage, and referenced bytecode;
3. for each committed block, join execution/state-change notifications with current
   state to emit final values rather than unwind/before-values;
4. assign and persist account incarnations across deletion/recreation;
5. emit bytecode no later than its first reference, one block-end event count, and a
   trailer;
6. retain observed ancestry through the selected gate and stop on any previously
   published number/hash conflict;
7. compare exporter fixtures with independent RPC/state expectations for the pinned
   Base genesis and Reth version.

The adapter must not infer these semantics from raw changeset tables without a
version-specific test.

## Security and correctness boundary

SHA-256 and frame decoding detect accidental corruption; code blobs are also checked
against Ethereum Keccak-256 during ingestion. They do not authenticate the publisher,
prove canonical consensus, or reconstruct/verify the supplied Ethereum state root.
Finality is inherited from the trusted checkpoint source. Protect bucket credentials,
TLS, the mutable head, and deletion permissions; enable provider versioning/retention
for recovery. See [limitations](docs/limitations.md) for the complete prototype
boundary.

Licensed under MIT.
