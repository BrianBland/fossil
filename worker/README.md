# Rust/WASM Cloudflare Worker

This crate is the Cloudflare Workers adapter for the single Fossil format-v1 archive.
All archive parsing, integrity checks, and Ethereum state queries are Rust compiled to
WASM. `worker-build` generates only the Workers JS/WASM loader; there is no maintained
JavaScript format decoder and no Worker-specific archive layout.

The checked configuration binds R2 bucket `fossil` as `ARCHIVE`, uses Base
chain ID `8453`, and reads under `v1/`. The live archive has an exhaustive
Base genesis anchor and contiguous finalized replay deltas, verified through
block 1,278,000 on 2026-09-23; query `eth_blockNumber` for its advancing head.

## API and limits

`POST /` and `POST /rpc` accept one JSON-RPC 2.0 request (maximum 16 KiB):

- `web3_clientVersion`
- `eth_chainId`
- `eth_blockNumber`
- `eth_getBalance`
- `eth_getTransactionCount`
- `eth_getCode`
- `eth_getStorageAt`

State methods accept canonical numeric quantities, `latest`, `safe`, `finalized`, and
`earliest` when the archive anchor is genesis. EIP-1898 `{blockNumber,
requireCanonical?}` is supported; block hashes, conflicting selectors, `pending`,
batches, and unknown methods are rejected. A fixed-offset publication explicitly
rejects `safe`/`finalized`. There are no `fossil_*` methods.

The edge reader serves both the active window and completed windows. Numeric historical
selection traverses the bounded two-level catalog (one root and one chunk), validates
the selected completed-directory range, and uses the window's base checkpoint when no
epoch delta contains the exact key. Explicit Ethereum absence/zero defaults are returned
only after an exact checkpoint miss.

Each lookup is bounded to 192 R2 GETs, 128 MiB fetched, and 128 MiB decoded. Epoch index
objects are fetched in newest-first waves of at most 12 concurrent GETs. Every wave's
known ObjectRef lengths and GET count are reserved atomically before any GET starts, and
results are still evaluated strictly newest-to-oldest so predecessor semantics do not
change. Format-v1 native publication and the Worker share an 8 MiB decoded/8.25 MiB
encoded data-object cap and a 2 MiB encoded index-object cap. Checkpoint partition
manifests select an adaptive power-of-two count from 8 through 256, while every emitted
checkpoint shard is limited to 32 MiB decoded and 33 MiB encoded; checked demo and real
measurement indexes are far below that cap, and oversize references fail before R2
I/O. The deterministic 65-epoch fixture's cold active-window storage fallback is eight
GETs; the hard per-request ceiling remains 192 GETs. Index parsing validates at most 65,536 entries, retains only up to 64
matching fingerprint candidates, and drops its local encoded-buffer copy before data
fetches. Checkpoint parsing validates the complete subshard with borrowed, sorted exact
keys and retains only the selected pointer rather than materializing its state map.
Pure-Rust `ruzstd` decoding keeps only borrowed run/key/value slices and clones
the one selected value; peak data-object ownership is the encoded object, one <=8 MiB
decoded buffer, and the selected value (plus ruzstd's bounded <=8 MiB window).
Immutable references verify exact length and SHA-256, routing verifies the full key
after its fingerprint, integer encodings must be
canonical, and code bytes are charged to the decoded budget before Keccak-256. The
per-request verified immutable cache is limited to 96 objects/16 MiB, with a 2 MiB
per-object limit. Cache hits recheck exact length and SHA-256 and allow account and
incarnation-qualified storage lookups in the same address-routed partition to reuse
index/data objects. No module-static cache is used: the current worker-rs execution
model does not provide a synchronization/lifetime contract strong enough for safely
bounded mutable cross-request WASM state, so the implementation does not pretend that
an isolate cache is authoritative or available.

`Reader::stats()` exposes deterministic per-request immutable GET, fetched-byte,
decoded-byte, cache-hit, cache-miss, and eviction counters for fixtures and experiments.
These counters are intentionally not response headers yet: plumbing timing and counters
through every JSON-RPC error path would be invasive, and test/benchmark counters avoid
exposing archive details in the public transport.

The read-only transport routes are `GET|HEAD /health` and
`GET|HEAD /objects/objects/sha256/<2-lowercase-hex>/<64-lowercase-hex>`. The shard must
match the digest prefix. The public gateway cannot expose publication heads or any
non-CAS key; only RPC internals read the mutable head. It streams bodies and supports
one byte range, strong ETags, `If-None-Match`, and strong-ETag `If-Range`. Valid but
unsatisfiable byte ranges return no-store 416; malformed ranges (including an end
before its start), multiple ranges, and non-byte ranges are ignored and return the full
200 response. There are no writes, deletes, lists, or
empty-key/listing operations. All error responses use `Cache-Control: no-store`.

## Reproducible build and direct deployment

The crate pins Rust 1.91.1 and the `wasm32-unknown-unknown` target in
`rust-toolchain.toml`; `Cargo.lock` pins Rust dependencies. Install the locked Worker
builder and Node deployment dependencies:

```sh
cd worker
rustup toolchain install 1.91.1 --profile minimal \
  --target wasm32-unknown-unknown
rustup component add rustfmt --toolchain 1.91.1
cargo install worker-build --version 0.8.6 --locked
npm ci
```

Then test and create the deployable loader plus WASM module:

```sh
npm run check
npm run build
npx wrangler deploy --dry-run --outdir .wrangler-dry-run
```

The deployable files are generated under `build/worker/` and are intentionally ignored.
The build uses worker-rs/worker-build 0.8.6 with panic recovery disabled because the
normal error paths are explicit and this avoids exception-wrapper requirements; an
unexpected Rust panic terminates that request. Review `wrangler.toml`, then deploy
directly (no hand bundling step):

```sh
npm run deploy
```

Wrangler builds first, uploads the generated JS/WASM module, binds `ARCHIVE` to
`fossil`, and supplies `IMMUTABLE_PREFIX` and `CHAIN_ID`. Never place R2
credentials in source or client URLs.

## Live deployment and `cast` checks

The canonical Rust/WASM deployment is live against bucket `fossil` under `v1/`.
Its Worker hostname stays anonymized in this repository; set `$FOSSIL_RPC` to
`https://YOUR-WORKER.workers.dev/rpc`. The exhaustive genesis package has
2,064 accounts, 2,075 nonzero storage slots, and 16 unique code blobs.
The [live demo](../docs/live-demo.md) records exact genesis checks and the
historical anchor inventory. The read-only [Base exporter](../tools/base-export/README.md)
appends contiguous replayed history under a 5 GB cap. This is a partial
archive, not a claim that all later Base state has been exported.

## Shared golden contract

`testdata/archive-fixture.json` is the same deterministic format-v1 object graph used
by native Rust Worker tests. It pins the native Fossil golden head layout and exercises
account, storage, cross-object index/data references, Zstd, and code lookup. A second
deterministic graph built by `tests/fixture.rs` pins a 65-epoch commit digest and covers
a completed 64-epoch window, a new active window, checkpoint fallback, tombstones,
incarnation-qualified storage, corruption, and cold-GET bounds. The Worker core is
platform-neutral Rust (`src/core.rs`), so native tests execute the exact decoder compiled
into WASM rather than a second test implementation.
