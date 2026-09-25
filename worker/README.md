# Rust/WASM Cloudflare Worker

This crate is the Cloudflare Workers adapter for the Fossil tiered v1 archive.
All archive parsing, integrity checks, and Ethereum state queries are Rust compiled to
WASM. Run, summary and head decoding come from the shared `fossil-codec` crate (built
without its native encoder), so the Worker decodes exactly the bytes the native
publisher writes. `worker-build` generates only the Workers JS/WASM loader.

The checked configuration binds R2 bucket `fossil` as `ARCHIVE`, uses Base chain ID
`8453`, and reads under `v1/`. **Do not deploy this version until `fossil` has been
reset and repopulated in tiered v1**: the currently live Worker and bucket use the
previous checkpoint format, which this reader does not understand.

## API and limits

`POST /` and `POST /rpc` accept one JSON-RPC 2.0 request (maximum 16 KiB):

- `web3_clientVersion`
- `eth_chainId`
- `eth_blockNumber`
- `eth_getBalance`
- `eth_getTransactionCount`
- `eth_getCode`
- `eth_getStorageAt`

State methods accept canonical numeric quantities, `latest`, `safe`, `finalized`
(all the published finalized head), and `earliest` (genesis). EIP-1898
`{blockNumber, requireCanonical?}` is supported; block hashes, `pending`, batches, and
unknown methods are rejected. There are no `fossil_*` methods.

Each request reads the mutable head once (the run manifest is inline), then probes runs
newest first. A run is skipped when its no-false-negative summary shard excludes the
key; an address's account and storage share one shard, memoized for the request.
Positives are verified in the run's exact fence tree (at most three GETs). Every GET is
charged before it is issued against hard limits of 192 GETs and 128 MiB fetched per
request; immutable objects verify exact length and SHA-256, summary shards verify their
run binding and trailing checksum, and code verifies Keccak-256. `Reader::stats()`
exposes per-request GET and fetched-byte counters for tests and experiments.

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

## Native parity

`tests/parity.rs` builds a real tiered store with the native publisher and compactor
(46 epochs, several compacted levels, tombstones, zero values, a destroy and
recreation), then checks that every Worker account, storage and code answer equals
the native reader's and that each cold account-plus-storage read stays within
`1 + runs + 6` GETs.
