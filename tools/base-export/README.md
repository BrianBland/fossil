# Base → Fossil bounded exporter

This tool replays Base blocks read-only, emits bounded `fossil-export/1` packages with a SQLite account-incarnation journal, and publishes immutable objects to R2 before a conditional head update. It is **not a genesis bootstrap**: start only after independently verifying the published system-only baseline at block 1,104,000 (all visible lifetimes have incarnation 0). Keep exactly one publisher and an idle, head-matched workspace when resuming.

## Build on a Base devbox

Requires Linux, a synced Base Reth datadir, Rust/Cargo compatible with Base v1.4.1-rc.11, Python 3, and `boto3` for publication. The standalone manifest and lockfile pin Base `v1.4.1-rc.11` and Base Reth `base-v2.5.2.6`; it does not modify the Base checkout. On the devbox, `cargo check --locked` and `cargo build --release --locked` succeeded against those Git dependencies. An initial build may download substantial dependencies.

From the Fossil repository root:

```sh
cargo build --locked --release --manifest-path tools/base-export/probe/Cargo.toml
cargo build --release --manifest-path Cargo.toml
python3 -m unittest discover -s tools/base-export -p 'test_*.py'
```

For a **separate verified baseline and idle target only**, create a workspace containing a copy of the prior Fossil mirror (`mirror/chains/0x2105/heads/finalized.bin` and `mirror/objects/sha256/...`) whose head is exactly `BASELINE`. Point the exporter at a read-only-capable Reth datadir; the probe opens its provider read-only, reexecutes each requested block from the preceding historical state, validates post-execution receipts, and emits state changes including `wiped_storage`. It does not start/stop a node. The RPC URL supplies chain/finality checks and the preceding canonical block hash. Set the following shell variables yourself (values here are intentionally unspecified):

```sh
export WORKSPACE=/path/to/separate/fossil-workspace
export BASE_DATADIR=/path/to/base-reth-datadir
export BASE_RPC=http://127.0.0.1:18545
export BASELINE=1104000
export FIRST=1104001
export LAST=1105000
export R2_BUCKET=your-bucket
export CF_S3_API_ENDPOINT=your-r2-s3-url
export CF_ACCESS_KEY_ID=your-access-key
export CF_SECRET_ACCESS_KEY=your-secret-key
```

Install the publisher dependency in that separate workspace (no system Python changes):

```sh
python3 -m venv "$WORKSPACE/.venv"
"$WORKSPACE/.venv/bin/python" -m pip install boto3
```

Preview a single epoch without R2 access or R2 writes (the existing workspace/mirror and a matching local head are still required):

```sh
"$WORKSPACE/.venv/bin/python" tools/base-export/export.py --first "$FIRST" --last "$LAST" --baseline "$BASELINE" \
  --workspace "$WORKSPACE" --datadir "$BASE_DATADIR" \
  --replay "$(pwd)/tools/base-export/probe/target/release/fossil-state-probe" \
  --fossil "$(pwd)/target/release/fossil" --rpc "$BASE_RPC" --bucket "$R2_BUCKET" --dry-run
```
The dry run leaves the normalized package at `$WORKSPACE/$FIRST-$LAST.jsonl` for inspection; it rolls back the incarnation journal and does not advance either publication head.

To deliberately publish after checking the preview, remove `--dry-run` from the same command. Publishing needs the three `CF_*` environment variables above. The mirror must byte-match the remote head. Each epoch contains at most 1,000 blocks; normalized input is limited to 100,000,000 bytes per epoch. **The entire local workspace must remain at or below 5,000,000,000 bytes (decimal 5 GB)** before and after staging; exceeding the cap stops before R2 publication. Neither source data nor remote bucket usage counts toward that local cap. Reconcile an interrupted/stale staged mirror and incarnation journal manually before retrying; do not reset the journal or overwrite a newer head.
The publisher separately checks the existing R2 prefix plus new immutable bytes against the same 5 GB limit before uploading. Use one writer; unrelated concurrent uploads invalidate that size forecast.

Publication stages once in the local Fossil store and checks that the new commit extends the prior one. It uploads immutable SHA-256 objects with up to **32** concurrent conditional `IfNoneMatch=*` PUTs (existing bytes are verified), then rechecks the remote head/ETag and performs the `IfMatch=<prior ETag>` head CAS **last**. The SQLite journal commits only after R2 confirms the head. This trusts Reth replay and finality RPC, not independent `state_root` reconstruction.

On the devbox, 160 isolated small conditional R2 PUTs took 12.63 seconds at 16 workers, 7.16 seconds at 32, and 6.37 seconds at 64. The 32-worker limit is a measured throughput/memory trade-off, not an end-to-end export guarantee.

Read-only replay uses at most eight concurrent 125-block chunks, then emits blocks in order. On the devbox, a 1,000-block sample at Base block 5,000,001 took 38 seconds serially versus 14.35 seconds with eight workers while the node remained live; this measures replay only, not end-to-end export or production R2 latency.
