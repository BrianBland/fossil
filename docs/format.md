# Fossil storage format version 1

Fossil archives complete EVM post-state history in immutable objects. One mutable JSON head, `chains/<chain-id>/heads/tiered-v1.json` (at most 256 KiB), is updated with compare-and-swap after every referenced object has been uploaded. The head carries the finalized block, canonical hash, state root, input digest, the complete run manifest inline, and a reference to an immutable copy of the head it replaced, so a cold reader needs exactly one GET before probing runs. An object reference contains its SHA-256 digest and exact encoded length. Readers verify both before decoding.

## Keys and values

```text
account  = 0x01 || address20
storage  = 0x02 || address20 || incarnation_u64_be || slot32
code     = 0x03 || keccak256(bytecode)
```

The namespace byte occurs exactly once. Full-key SHA-256 routes index lookups, so the storage of one large contract can span many partitions. Every encoded account/storage value is associated with its exact full key and the block after whose execution it became effective. Account records include existence, nonce, balance, code hash, and incarnation. Account tombstones and zero storage values are versions, not absent records. A recreated account receives a new incarnation; historical slots of earlier incarnations remain readable at earlier blocks. Code bytes are immutable, deduplicated objects checked against their Keccak code hash.

The first sealed package may be an exhaustive anchor containing all live accounts and nonzero slots. Every subsequent package is a contiguous successor and contains only changes. The archive cannot answer blocks before the anchor. Epochs cover at most 1,000 contiguous finalized blocks; a short epoch may be sealed for publication freshness. Each epoch is retained as the canonical source of its normalized post-state events and block metadata.

## Time-disjoint index runs

The authoritative point-lookup index contains recent epoch runs and older compacted runs. Within a run, exact `(full_key, block)` records are sorted lexicographically and by ascending block, and contain the post-state value. Run block ranges are disjoint; all versions, including deletions and zeros, survive compaction. A byte-bounded builder merges whole adjacent ranges into the next level by streaming sorted records rather than materializing all live state. Unchanged canonical epoch objects remain durable; obsolete derived runs are not publication inputs after compaction.

Publication appends each sealed epoch as one L0 run and never compacts. A separate compactor folds the oldest four L0 runs, together with every full level they cascade through, into one run using a single streaming k-way merge. Level `i` holds at most one run whose weight (in epochs) is a multiple of `4 * 8^i` and below `4 * 8^(i+1)`. Compaction commits by CAS against the current head, keeping any L0 runs published meanwhile; publication rebases onto a compacted head with the same block. At most 16 L0 runs may await compaction; beyond that publication fails with a backlog error rather than making reads unbounded. Steady state is at most three L0 runs plus one run per occupied level, so run count grows logarithmically with archive age.

Each run has an exact `(full_key, block)` fence tree over 16 full-key-hash partitions. A fence uses `upper_bound((key, requested_block))` to locate the predecessor, verifies the full key and block, and returns the encoded value. The tree splits pages and routes hot keys by `(key, block)` so the number of versions for one key cannot enlarge one object without bound. Fence pages are at most 512 KiB, state pages and code blobs at most 1 MiB. A publisher fails before head CAS if it cannot satisfy the reader's bounds.

Each run also has no-false-negative Bloom summaries (16 bits and 11 hashes per key, about 64 KiB and 32,768 keys per shard, at most 1 MiB) at deterministic paths `summaries/<run-root-hex>/{a,s}<index>`. The shard count is a power of two recorded in the head. Address shards are routed by the 20-byte address, so an account and all of its storage share one shard and one GET answers both halves of an account-plus-storage read. An address with more than 4,096 keys in one run instead sets a marker in its address shard and places its keys in full-key-routed spill shards. Each shard binds its run digest, kind, index and shard count and ends with a SHA-256 of its preceding bytes; a missing or mismatched shard is an error, never an absence. Readers probe runs newest to oldest, skip runs whose summary excludes the key, and verify every positive in the fence tree. Cold account-plus-storage cost is therefore one head GET, one summary GET per run (two for heavy addresses), and at most three fence/data GETs per hit, plus false positives.

## Point lookup and cache

An RPC request pins one verified commit. To read account or storage at block B, select the newest qualifying version with `version.block <= B` across runs whose ranges can contribute; absent keys resolve to Ethereum defaults. Storage resolves the account at B first and uses its incarnation when constructing the slot key. Code resolves the account, then the code hash and immutable blob. Hash selectors require a separately committed bounded hash-to-number catalog; numeric/tag state reads do not scan block catalogs or old commits.

Immutable objects may be cached by verified digest in a bounded request-local, process, or edge cache. Caches are disposable and never change the answer. Frequently read state leaves fit the Worker's 2 MiB per-item cache limit. The mutable head has separate freshness semantics and must not be treated as an immutable cached object. The cold-cache p99 target for account plus storage is at most 20 R2 object GETs on representative busy and old-block workloads; warm-cache performance is measured separately. A request still has hard 192-GET and 128-MiB fetched/decoded resource limits, enforced before I/O and without relying on cache hits.

## Publication, recovery and retention

The writer verifies source chain identity, finalized continuity, package hashes and account/code lifecycle before publication. It records staged object references and compares the current remote head before CAS; a failed or retried upload cannot expose a partial run. Immutable object keys are `objects/sha256/<first two digest hex>/<64 digest hex>`. Reconciliation after a crash starts with the remote head rather than assuming local journal progress. The exporter may replay finalized chunks in parallel but commits them in block order.

The live head and explicitly pinned rollback heads are GC roots. Canonical epoch data and code are retained; only unreachable superseded derived index objects and abandoned staged objects may be collected after a grace interval longer than the maximum request and upload lifetime. Readers use the latest committed index to serve all historical blocks, so old derived index generations are not required after the rollback interval.
