# Storage formats

## Compatibility

V1 is frozen and remains tested. V2 epoch storage is incompatible with both v1 and
the rejected, never-committed state-key COW prototype. The tuned epoch encoding uses
binary version 3 (128/32/8 partitioning and no epoch-63 checkpoint). Its mutable head is
`chains/<chain-id>/heads/finalized-v2-epochs.bin`; readers never fall back to another
format or old v2 key.

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
commit reference must equal the fixed commit size. The fixed-size mutable v2 head also
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

The 96-bit candidate fingerprint remains `SHA256(namespace || full logical key)`.
Account and storage primary partitions use the top seven bits of `SHA256(address)`, so
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
epoch descriptors. Its start must match the commit, and its last epoch must end at the
committed published block. The only empty-directory exception is immediately after a
rollover, where `active_window_start == published_number + 1` using checked arithmetic. It is a bounded object rewritten once per epoch. A reader may scan
at most 64 epoch deltas. At the 64th epoch (64,000 blocks), publication builds exactly
one closing checkpoint:

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

For block B, a reader lazily selects the active or completed 64,000-block directory,
computes one router partition, and searches applicable epoch index deltas newest to
oldest (at most 64 after the base checkpoint). Fingerprint candidates are accepted
only after full-key verification in a data run. If no post-checkpoint version exists,
the exact checkpoint partition points directly to the current non-default version;
otherwise the Ethereum default applies. Thus a value unchanged for millions of
blocks resolves through one checkpoint pointer rather than publication history.
Storage first resolves the account incarnation, then performs its co-partitioned slot
lookup. Numeric/tag selectors are supported. V2 EIP-1898 block-hash selectors fail
explicitly until a durable bounded hash-to-number index exists; readers never scan all
epoch block objects.

Readers may lazily cache verified directory, index, checkpoint, and data objects. This
cache is bounded, disposable, and never authoritative; readiness does not ingest
history. One logical RPC lookup has hard internal limits of 192 remote object GETs and
128 MiB total decoded bytes. Data/checkpoint objects have 64 MiB hard decoded limits;
checkpoint subshards have a 32 MiB construction target. Limit exhaustion fails closed.
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
