# Prototype format

All durable semantic documents use compact serde JSON with struct field order; arrays
are explicitly sorted. This serialization is the canonical byte representation for
schema version 1. Unknown schema values fail closed.

## Input

A package begins with one `header`, has contiguous `block` groups, and ends with one
`trailer`. Header mode is `anchor` with `anchor_number`/`anchor_hash`, or `delta` with
`preceding_number`/`preceding_hash`. Block events are `account`, `storage`, and `code`.
Exactly one `block_end` is required before the next block or trailer and checks the
event count. Unknown fields, including the unsupported `events_sha256`, are rejected
rather than presented as integrity checks. See `tests/fixtures/minimal.jsonl` and the
README for the field contract.

## Segment

`fossil-segment/1` contains chain identity, inclusive block bounds, canonical block
metadata, and sorted vectors of account/storage/code events. The canonical JSON is
compressed as one deterministic zstd frame at level 9. A descriptor records:

- SHA-256 and length of the exact compressed bytes;
- SHA-256 of canonical decoded bytes;
- codec (`json+zstd`) and inclusive block range;
- index SHA-256 and exact length.

The 512 MiB bounded decoder limit is a prototype safety cap. Production segmentation
should create independently authenticated smaller frames and use range reads.

## Index and manifest

`fossil-index/1` is a sorted list of `(namespace, logical key, min block, max block)`
rows plus the segment and decoded hashes. It routes a key directly to candidate
segments; serving mirrors and verifies all indexes before readiness.

`fossil-manifest/1` includes generation, chain identity, bootstrap boundary, published
head/state root, parent manifest digest, gate description, input digest, and the full
ordered segment catalog. The canonical manifest bytes are SHA-256 addressed. Schema
upgrades require a new schema string and decoder; version 1 readers do not guess.

Every immutable object key is:

```text
objects/sha256/<first two digest hex>/<64 digest hex>
```

The digest covers exact stored bytes. ETags are never content hashes. The only mutable
object is `chains/<canonical chain quantity>/heads/finalized.json`, which records the
manifest digest, generation, number, and hash.
