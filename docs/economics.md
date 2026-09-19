# Request economics and scale evidence

Hot execution storage and finalized immutable history have different I/O needs.
Execution nodes need low-latency local disks for sync and execution. Sealed history can
live in cheaper centralized object storage while nodes prune and disposable readers
share one R2/S3 backend.

V2 startup is one mutable-head GET and one immutable-commit GET. It performs no LIST,
parent walk, directory ingestion, or state read. A first lookup lazily fetches its
window directory, one router-delta partition per applicable epoch, fingerprint
candidate data, and possibly one checkpoint partition manifest/subshard/data pointer.
There are at most 64 post-checkpoint epoch deltas in a window. Completed-window
selection costs a bounded catalog root plus one numeric chunk, independent of archive
age. Verified immutable objects are cached
locally, so repeated lookups can avoid remote GETs; the cache is not authoritative.
Storage lookup additionally resolves the account incarnation.

R2 Standard is the recommended default for interactive reads. S3-compatible storage
is portable. Provider request, storage, retrieval, and egress prices and cache behavior
must be measured in the deployment region. Workers are future stateless readers;
Durable Objects are not required for bulk storage or reads.

## Why the state-key COW design was rejected

The exact 100,000-block/1,000-block-chunk COW run created **929,134 immutable files**
(about **3 GB logical and 5 GB allocated**) before exhausting the **1,048,576 `/tmp`
inode** limit. A rerun on md0 accumulated **26 GB** and was still unfinished after
**15 minutes**. This failed the scale gate even though small point benchmarks looked
reasonable. It is retained as rejected-design evidence and must not be presented as
production or current-v2 performance.

The tuned sealed-epoch design caps ordinary epoch fanout at 32 data and 128 index-delta
objects plus block/directory/commit objects. Exactly one closing checkpoint is emitted
after epoch 64, with at most 128 partition manifests and 8 subshards per partition.
The manual benchmark records cumulative objects, files, bytes, and elapsed time after
every epoch rather than assuming the scale target passes.

A read-only StaticFileProvider HLL measurement over the real Base 100k range estimated
940,971 unique accounts and 17,339,777 unique address/slot keys across the measured
19,866,464/88,534,816 rows. The corrected generator preserves local 1,000-block pools
of 20,614/244,196 and slides them to those approximate global unions. Summary SHA-256:
`e00f51a7cc9be49a2560342c20df44f15bc8dd2a53285eab1bb84042f2fccd8a`.

The corrected Base-cardinality-shaped 100,000-block run completed all 100 epochs in
228.786 seconds with 17,456 immutable objects and 1,141,404,991 logical immutable
bytes. It passed the prototype gates of ten minutes, 35,000 objects, and 2.75 GB.
The compact checked result is `benchmarks/results/v2-base-shaped-epoch-100k.json`;
the full progress result has SHA-256
`a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.
It is filesystem-backed synthetic cardinality-shaped evidence, not production R2
performance or semantically complete forward state.

The earlier 425.3685-second high-cardinality run and the 505.440-second 256/64/16
fanout run are rejected tuning evidence, not current-format results.

## Reproduction

Checked 1,000-block epoch:

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/v2-base-shaped-epoch.json
```

Manual chunked run (explicit scratch, no hidden `/tmp`):

```bash
cargo run --release -- benchmark \
  --manual-blocks 100000 --chunk-blocks 1000 \
  --scratch-dir /mnt/md0/fossil-epoch-bench \
  --output benchmarks/results/manual-v2-100000.json
```

Manual mode canonicalizes and rejects system `/tmp` descendants, requires an empty
explicit store and at least 8 GiB free, generates and drops one chunk at a time, and
atomically checkpoints JSON progress after every sealed epoch. Each progress row
records cumulative elapsed seconds, current available filesystem bytes, and available
inodes when `df -Pi` output is parseable. Targets are at most
35,000 immutable objects excluding code, 2.75 GB immutable bytes, and ten minutes on
the documented devbox. A run reports failed or incomplete rather than fabricating a
pass.

Before a production claim, measure data/index/checkpoint/directory/catalog bytes,
cache-warming-sequence and locally ingested GETs/bytes, cache capacity and eviction, provider
latency/throttling, open-epoch build resources, checkpoint rollover, unreachable
orphan retention, and offline inventory/GC cost.
