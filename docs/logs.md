# `eth_getLogs` v2 design (deferred)

Log serving is not implemented in this pass. This design is intentionally separate
from the sealed state-epoch namespace.

## Namespace and pages

Receipts and logs describe block outputs, while state history describes values keyed
by account/slot at a historical point. Their access patterns, retention units, query
filters, and canonical ordering differ. Mixing logs into state leaves would inflate
ordinary state reads and couple independent page-size/index evolution. Logs and
receipts therefore get a separate immutable namespace and roots.

The primary representation will be bounded-range, **block-major log pages**. Each
page covers a contiguous block interval and stores canonical tuples sufficient to
return Ethereum log objects. Records are ordered exactly by `(block_number,
transaction_index, log_index)`; reassembly must preserve that canonical order.
Receipts can share block-range boundaries but remain independently addressed.

## Epoch-sharded immutable indexes

Publication creates immutable compressed bitmap/posting indexes for emitting address
and topic value at each topic position 0 through 3. Every posting is sharded by a
fixed **8,192-block epoch**. A key with activity over the full chain therefore has
many independently bounded shards, never one history-sized object.

Each publication commits bounded-height immutable directory pages. A root directory
selects an epoch range, a second level selects one epoch, and a leaf directory maps
`(kind, address-or-topic, topic-position)` to a bounded posting object. Directory
fanout and encoded page size are fixed, so lookup height does not grow with the number
of publications. Empty epochs require no posting object. Exact limits and epoch size
must be frozen in the eventual format version after measurement.

Index postings identify blocks or bounded block-page positions. Query evaluation uses:

1. select intersecting epoch shards through the immutable directories;
2. OR within the address list;
3. OR among alternatives at each requested topic position;
4. intersect the address result and every constrained topic position per epoch;
5. scan candidate block-major pages to remove bitmap false positives and apply exact
   block/hash bounds;
6. merge results in exact `(block_number, transaction_index, log_index)` order.

An empty alternatives list follows Ethereum filter semantics rather than silently
matching everything. Block-hash filters select one canonical block and cannot be
combined with a range.

A filter with no address or topic index scans the block-major pages for its bounded
block range directly. It never creates or fetches a universal posting. The same block
span, page, byte, log, and GET limits apply to filtered and unfiltered scans.

## Required request caps

The first implementation must expose deterministic resource-limit errors and start
with conservative hard caps no greater than:

- 10,000 inclusive blocks per request;
- 64 directory/index shards and 8 MiB encoded index bytes;
- 256 candidate block-major pages;
- 64 MiB total decompressed page bytes;
- 10,000 returned logs and 16 MiB encoded response bytes;
- 384 backend GETs, including directories, postings, and data pages.

A request exceeding a cap fails as a whole; it must never return a silent partial
result. Provider traces may justify lower defaults or an explicit pagination API.
Increasing one cap requires re-evaluating every other bound.

## R2 and Worker request behavior

A reader obtains bounded directory and epoch-posting objects, intersects locally, then
GETs only candidate bounded-range data pages. Adjacent candidates can be coalesced
only within the byte/page caps. R2 Standard is the default backing store.

A future stateless Cloudflare Worker can perform the same immutable
GET/intersection path and stream a size-limited response. It needs no Durable Object
for bulk storage or read coordination. Worker CPU/time and subrequest limits are
additional deployment caps. No Worker deployment code is included yet.

## Validation still required

Before implementation, differential fixtures must cover multi-address and per-topic
OR semantics, topic-position intersection, epoch boundaries, removed-log policy,
EIP-1898/block-hash selection, unfiltered scans, response field encoding, exact
ordering, empty blocks, each resource cap, and provider request/byte bounds.
Publication must prove each posting refers only to committed log pages and corruption
must fail closed.
