# Fossil storage format version 1

This is the initial Fossil on-disk format. Every binary object carries schema version
1. There is one mutable head, `chains/<chain-id>/heads/finalized.bin`, and one
immutable object namespace. Readers do not probe alternate keys or formats.

The fixed layout uses 128 router/checkpoint partitions, 32 epoch data partitions,
eight checkpoint subshards per primary partition, epochs of at most 1,000 blocks, and
windows of 64 sealed epoch descriptors. The immutable references make this object
hierarchy a [Merkle DAG rooted at the commit digest](integrity.md).

## Head and commit

The binary mutable head identifies one fixed-size immutable commit. The commit has
constant size and contains chain, anchor, published-head, finality, input, audit-parent,
active-window-directory, and completed-window-catalog metadata. Ordinary startup is
exactly head plus commit. The audit parent is validated as a reference but never
traversed by startup or queries.

All immutable references contain SHA-256 and exact byte length. Before buffering,
readers reject reference lengths over the object-type bound and require filesystem,
memory, or provider metadata to fit the bound; provider metadata and returned length
must agree. Bytes are then checked against the exact reference before parsing. The
commit reference must equal the fixed commit size. The fixed-size mutable format head also
uses a bounded read: filesystem reads stop at `max+1`, and object-store streams abort
as soon as accumulated bytes exceed the cap even if metadata underreports. Decoders
reject bad magic/version,
trailing bytes, invalid references, noncanonical ULEB128/U256, zero-version runs,
impossible allocation counts, unsorted keys/versions, and out-of-range offsets.

## Sealed epochs

A producer buffers a maximum 1,000 contiguous blocks and publishes only the sealed
epoch; no open epoch is mutable or visible. The first exhaustive anchor is a special
sealed epoch/checkpoint. Within an epoch, changes are grouped by exact logical key and
versions are block-delta encoded in ascending order. Account tombstones, explicit
zero storage, and incarnations retain their existing semantics. Code bytes remain
separate deduplicated CAS objects with a documented 1 MiB prototype hard limit.

The 96-bit candidate fingerprint is
`SHA256(full encoded key)[0..12]`, equivalently
`SHA256(namespace || logical payload)[0..12]`. The full encoded key already begins
with its namespace byte; the namespace is never hashed twice. Account and storage
primary partitions use the top seven bits of `SHA256(address)`, so
an account and every storage incarnation share one of 128 checkpoint partitions. Code
uses the equivalent code-hash route. Four primary partitions share each of 32 epoch
data objects. Every nonempty data object contains sorted full keys and versions in bounded
binary form, compressed with deterministic Zstd level 9.

Every nonempty router partition has a compact index-delta object. Entries contain only
a 96-bit route fingerprint, first-change block, data-object dictionary index, and run
offset. A reader requires every dictionary reference it follows to exactly equal the
corresponding data reference committed in that epoch descriptor, then compares the
complete candidate key. Thus a malicious redirect or fingerprint collision fails
closed or only adds candidate work; it cannot return another object/key.

## Windows and checkpoints

One active-window directory identifies its base checkpoint and at most 64 sealed
epoch descriptors. Each descriptor covers at most 1,000 blocks, so a full window
covers **at most** 64,000 blocks; short tail/fixture epochs make it smaller. Its start
must match the commit, and its last epoch must end at the committed published block.
The only empty-directory exception is immediately after a rollover, where
`active_window_start == published_number + 1` using checked arithmetic. It is a
bounded object rewritten once per epoch. A reader may scan at most 64 epoch deltas.
After the 64th descriptor, publication builds exactly one closing checkpoint:

1. seals and retains the completed-window directory;
2. builds each of 128 primary checkpoint partitions independently, never one global
   state map;
3. filters storage pointers using the account's final existence/incarnation;
4. shards each primary partition into 8 full-key-hash subshards and emits a small
   partition manifest plus global checkpoint manifest;
5. appends the completed window to a numeric catalog chunk; and
6. starts a new empty active directory based on that checkpoint.

Each checkpoint subshard targets at most 32 MiB decoded and hard-fails above 64 MiB.
The global checkpoint manifest commits the boundary block. A checkpoint pointer is
accepted only when its version is at or before that boundary and the next version is
absent or after it. Manifest boundaries must match the directory base/tail boundary.
A query fetches one global manifest, one partition manifest, one subshard, and its
pointed data object. The completed-window catalog is two-level: a bounded root of at
most 4,096 chunk references and chunks of at most 256 numerically sorted windows.
Selection is always root plus one chunk regardless of archive age. Capacity is
1,048,576 completed windows; publication fails closed rather than growing the root.
State/data epochs, deltas, checkpoints, completed directories, catalog objects, and
code remain durable.

## Exact lookup

For block B, a reader lazily selects the active directory from the commit or a
completed directory from committed numeric catalog ranges. Selection uses encoded
start/end ranges; it never assumes `floor(block / 1,000 / 64)` because epochs may be
shorter than 1,000 blocks. The reader computes one router partition and searches
applicable epoch index deltas newest to
oldest (at most 64 after the base checkpoint). Fingerprint candidates are accepted
only after full-key verification in a data run. If no post-checkpoint version exists,
the exact checkpoint partition points directly to the current non-default version;
otherwise the Ethereum default applies. Thus a value unchanged for millions of
blocks resolves through one checkpoint pointer rather than publication history.
Storage first resolves the account incarnation, then performs its co-partitioned slot
lookup. Numeric/tag selectors are supported. Fossil EIP-1898 block-hash selectors fail
explicitly until a durable bounded hash-to-number index exists; readers never scan all
epoch block objects.

Readers may lazily cache verified directory, index, checkpoint, and data objects. This
cache is bounded, disposable, and never authoritative; readiness does not ingest
history. One logical RPC lookup has hard internal limits of 192 remote object GETs and
128 MiB total decoded bytes. Native data/checkpoint objects have 64 MiB hard decoded
limits; the isolate-constrained Rust/WASM Worker applies stricter 8 MiB decoded/8.25
MiB encoded data-object and 2 MiB encoded index limits without changing the format.
Checkpoint subshards have a 32 MiB construction target. Limit exhaustion fails closed.
Process-wide concurrency remains an operator limit.

## Publication and retention

All epoch/data/index/checkpoint/catalog/code objects are uploaded create-only before
the commit. The mutable head is changed last with CAS. Interrupted writers can leave
unreachable objects but cannot expose an incomplete epoch. There is no
publication-time deletion. Superseded active directories, commits, and orphans may be
removed only by a future offline inventory/GC process after an operator-defined
rollback window. Provider inventory and automatic GC are not implemented.

Immutable keys remain:

```text
objects/sha256/<first two digest hex>/<64 digest hex>
```
