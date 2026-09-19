# Request economics

Normal serving performs no LIST and no HEAD requests. Startup performs one mutable
head GET, one manifest GET, and one GET per index not already in the verified disk
cache. A cold point lookup fetches at most one whole candidate segment in this MVP;
storage/code may first need an account segment, but package-level co-location often
makes that the same object. A valid warm memory or disk hit performs zero remote
requests. Metrics count object GETs, transferred bytes, memory/disk hits, misses,
corruption, total immutable segment/index bytes, and segment count. The latter two
also support future fractional-storage capacity simulations.

The prototype stores one segment per eligible input package. That keeps publication
simple but can produce excess read amplification for large packages. Before making a
cost/performance claim, benchmark deterministic smaller independently compressed
frames and record:

- blocks and account/storage/code change counts;
- hot/cold key distribution, hit rate, and cold versus warm cache;
- segment compressed/decoded sizes;
- GET/HEAD/LIST/PUT counts and retries;
- transferred bytes and read amplification;
- cache size and eviction state;
- provider region, current request/retrieval/egress prices, and retention tier.

R2 Standard is the recommended starting point for high-GET, Internet-served RPC due
to its egress model. S3 Glacier Instant Retrieval should be considered only for
measured genuinely cold large objects; retrieval and minimum storage-duration costs
make it unsuitable as the default interactive tier. Indexes and hot segments belong
on local SSD. No claim of lower production cost is made without such a workload.

## Reproducible local headline

The checked artifact at `benchmarks/results/2026-09-19-local.json` is produced by:

```bash
cargo run --release -- benchmark \
  --output benchmarks/results/2026-09-19-local.json
```

It uses a fixed seed, 256 synthetic accounts, 32 cold point reads, 2,000 warm reads,
a memory backend, and concurrency one. Backend reads are counted by a benchmark-only
store decorator. Cost fields state their date, assumptions, and formulas and are
illustrative rather than production claims.
