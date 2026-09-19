# Request economics and scale evidence

Hot execution storage and finalized immutable history have different I/O needs.
Execution nodes need low-latency local disks for sync and execution. Sealed history can
live in cheaper centralized object storage while nodes prune and disposable readers
share one R2/S3 backend.

Fossil startup is one mutable-head GET and one immutable-commit GET. It performs no LIST,
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
production or implemented-format performance.

The tuned sealed-epoch design caps ordinary epoch fanout at 32 data and 128 index-delta
objects plus block/directory/commit objects. Exactly one closing checkpoint is emitted
after the 64th descriptor in a window, with at most 128 partition manifests and 8
subshards per partition.
The manual benchmark records cumulative objects, files, bytes, and elapsed time after
every epoch rather than assuming the scale target passes.

A read-only StaticFileProvider HLL measurement over the real Base 100k range estimated
940,971 unique accounts and 17,339,777 unique address/slot keys across the measured
19,866,464/88,534,816 rows. Summary SHA-256:
`e00f51a7cc9be49a2560342c20df44f15bc8dd2a53285eab1bb84042f2fccd8a`.

The real 1,296,000-block month range contained 277,758,377 account rows and
1,154,636,022 storage rows. HLL estimated 7,956,411 unique accounts and 218,828,772
unique address/slot keys; summary SHA-256 is
`42c965456b49243378b4c106c99ee6be50a40611adff5bf8232e1ab3bf5b511e`.
Reth changeset data plus offsets totaled 93,831,464,168 bytes
(11,759,303,278 account and 82,072,160,890 storage); summary SHA-256 is
`800390687bcea3ebc1b24dc50589b1708d246af5550b23a561234b4a1b5a1011`.

Layout-only processing of 1,296 real 1,000-block chunks took 814.453 summed seconds
across five resumable slices. Block-major Zstd-9 was 24,476,613,098 bytes and
key-major was 20,030,070,751 bytes (15,455.30 bytes/block, 18.1665% smaller); summary
SHA-256 is `15fb23b5244a74c3896cfc2cf6aa51a06e1470d0f28b11daf286981cbea2434c`.
This is real layout evidence, not end-to-end Fossil archive performance.

The generator preserves local 1,000-block pools of 20,614/244,196 and interpolates
piecewise through the measured 100-epoch and 1,296-epoch unions while preserving
consecutive overlap and staying inside the month ID universes.

The corrected Base-cardinality-shaped 100,000-block run completed all 100 epochs in
228.786 seconds with 17,456 immutable objects and 1,141,404,991 logical immutable
bytes. It passed the prototype gates of ten minutes, 35,000 objects, and 2.75 GB.
The compact checked result is `benchmarks/results/base-shaped-epoch-100k.json`;
the full progress result has SHA-256
`a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.
It is filesystem-backed synthetic cardinality-shaped evidence, not production R2
performance or semantically complete forward state. This historical completed run
predates the month-universe storage-address mapping and is not claimed as current
100k-generator output without a rerun.

The earlier 425.3685-second high-cardinality run and the 505.440-second 256/64/16
fanout run are rejected tuning evidence, not implemented-format results.

## Reproduction

Checked 1,000-block epoch:

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/base-shaped-epoch.json
```

Manual chunked run (explicit scratch, no hidden `/tmp`):

```bash
cargo run --release -- benchmark \
  --manual-blocks 100000 --chunk-blocks 1000 \
  --scratch-dir /mnt/md0/fossil-epoch-bench \
  --output benchmarks/results/manual-100000.json
```

The deterministic Base-cardinality-shaped month stress test completed all 1,296
1,000-block epochs in **9,449.0865 seconds (2h37m29s)**. It exercised 20 completed
64-epoch windows plus 16 active epochs and produced **234,368 immutable objects** and
**23,277,861,710 logical immutable bytes**: 41,472 data, 165,888 index, 23,060
checkpoint, 1,316 directory, 40 catalog, and 1,296 commit objects, plus the block
metadata objects included in the total. The compact checked result is
[`../benchmarks/results/base-shaped-month.json`](../benchmarks/results/base-shaped-month.json);
the full progress result has SHA-256
`c72c2960789cbf6000c42624dacaab03d70939a239747149dc3825d901c977c4`.

At an assumed R2 Standard storage price of **$0.015 per decimal GB-month**, its
23.2779 GB is approximately **$0.349/month for storage only**. This arithmetic excludes
requests, compute, cache, code traffic, retries, egress, production anchors, and price
changes. The run used the filesystem backend; it is not production R2 performance or
billing evidence. Its synthetic workload reproduces measured change counts and
approximate key cardinalities, not authoritative or semantically complete forward EVM
state.

Manual mode canonicalizes and rejects system `/tmp` descendants, requires an empty
explicit store and at least 8 GiB free, generates and drops one chunk at a time, and
atomically checkpoints JSON progress after every sealed epoch. Each progress row
records cumulative elapsed seconds, current available filesystem bytes, and available
inodes when `df -Pi` output is parseable. A run reports failed or incomplete rather
than fabricating a pass.

Before a production claim, measure data/index/checkpoint/directory/catalog bytes,
cache-warming-sequence and locally ingested GETs/bytes, cache capacity and eviction, provider
latency/throttling, open-epoch build resources, checkpoint rollover, unreachable
orphan retention, and offline inventory/GC cost.
