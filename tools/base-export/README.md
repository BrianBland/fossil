# Base → Fossil tiered exporter

This tool replays Base blocks read-only from genesis, emits bounded `fossil-export/1` packages with a SQLite account-incarnation journal, and appends each package to a tiered v1 archive with `fossil archive`. A separate `fossil compact --follow-seconds N` process folds L0 runs into levels; publication never waits for compaction except when the 16-run L0 backlog is full, in which case the exporter retries every 10 seconds.

## Build on a Base devbox

Requires Linux, a synced Base Reth datadir, Rust/Cargo compatible with Base v1.4.1-rc.11, Python 3, and `boto3` for remote size checks. From the Fossil repository root:

```sh
cargo build --locked --release --manifest-path tools/base-export/probe/Cargo.toml
cargo build --release --manifest-path Cargo.toml
python3 -m unittest discover -s tools/base-export -p 'test_*.py'
```

## Run

Set these yourself (values intentionally unspecified); the exporter maps the `CF_*` variables to the S3 settings `fossil` reads:

```sh
export WORKSPACE=/path/to/separate/fossil-workspace
export BASE_DATADIR=/path/to/base-reth-datadir
export BASE_RPC=http://127.0.0.1:18545
export STORE=s3://your-bucket/your-prefix
export CF_S3_API_ENDPOINT=your-r2-s3-url
export CF_ACCESS_KEY_ID=your-access-key
export CF_SECRET_ACCESS_KEY=your-secret-key
```

1. Publish the block-zero anchor built by `fossil anchor --genesis base.json --block block0.json --output anchor.jsonl`:
   `fossil archive --input anchor.jsonl --store "$STORE" --chain-id 0x2105 --gate finalized --finalized-head 0:<genesis-hash>`.
2. Start the compactor: `fossil compact --store "$STORE" --chain-id 0x2105 --follow-seconds 5`.
3. Start the exporter with a fresh workspace (the journal must be empty for `--first 1`):

```sh
"$WORKSPACE/.venv/bin/python" tools/base-export/export.py --first 1 --last "$LAST" \
  --workspace "$WORKSPACE" --datadir "$BASE_DATADIR" \
  --replay "$(pwd)/tools/base-export/probe/target/release/fossil-state-probe" \
  --fossil "$(pwd)/target/release/fossil" --rpc "$BASE_RPC" --store "$STORE"
```

Add `--dry-run` to convert one epoch and roll back the journal without writing. Each epoch contains at most 1,000 blocks and 100,000,000 normalized bytes. The local workspace and, every 25 epochs, the remote prefix (including superseded compaction output, which is not yet garbage-collected) must stay at or below `--cap-bytes` (default 5,000,000,000); exceeding either stops the export.

Resumption is safe after a crash: the journal commits only after `fossil archive` succeeds, and re-publishing the same package is idempotent. Keep exactly one exporter per store; the compactor may run concurrently. This trusts Reth replay and finality RPC, not independent `state_root` reconstruction.

Read-only replay uses at most eight concurrent 125-block chunks, then emits blocks in order.

## Measure

`fossil probe --store "$STORE" --chain-id 0x2105 --input samples.jsonl` opens a fresh, cold reader per `{"address","slot","block"}` sample and reports p50/p99/max GETs and bytes for the account-plus-storage read.
