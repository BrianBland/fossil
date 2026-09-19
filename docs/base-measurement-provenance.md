# Base measurement provenance

These retained summaries were sampled on **2026-09-19** from finalized Base head
**51,498,020**. The machine was `devbox` with **32 cores, 128 GiB RAM, and a 15 TiB
RAID**. The node image was `ghcr.io/base/node:v1.3.0-rc.6`; its Base Reth source was
tag `base-v2.5.2.6`, commit `5877708b`.

## Extraction methodology

Block and receipt shape came from canonical RPC block/receipt responses at the sampled
finalized head. State changeset shape and physical sizes came from a read-only Reth
`StaticFileProvider` using the state changeset offset/data files (`csoff`/`off`). No
node database was mutated. The 100,000-block layout run processed 100 real 1,000-block
chunks and discarded each decoded chunk after encoding its layout outputs.

The retained compact summaries have these SHA-256 digests:

| Summary | SHA-256 |
|---|---|
| `base-1000-rpc` | `72ce7fc51f252c4d0bcda1b624a092f06f9ab7d91043962afd97448e8ef63698` |
| `base-1000-state` | `2bae2a63e1864193fca2b1bf90fae2947e387766a66dc903faa65e156e96f034` |
| codec sizes | `866a8ff20d4baf36135c5d8ce9795c84e58288557023c047b2930db0cfa8951a` |
| codec timing | `0017d8d0ca6939742280b66e3d5d5644cab2410a628145fe4bf618a2f67cc957` |
| 100k layout | `d35bc9aaf183537f332a6687089f349060d21d9650193e719c196b7438b7f01b` |

## Exact ranges and retained results

- Blocks **51,497,021..51,498,020**: 161,699 account changes, 20,614
  unique accounts, 4,639 tombstones; 851,445 storage changes, 244,196
  unique slots, 169,441 zero values; Reth data plus offsets 67,895,321
  bytes. The prior compact key-major Zstd-9 experiment was 13,678,118 bytes.
- Blocks **51,398,021..51,498,020**: 19,866,464 account changes and
  88,534,816 storage changes; Reth data plus offsets 7,166,841,936 bytes.
  The 100 chunks produced block-major Zstd-9 at 1,854,933,204 bytes and
  key-major Zstd-9 at 1,504,915,953 bytes (15,049.16 bytes/block, 18.87%
  below block-major, 4.76x below the Reth representation), in 67.26 seconds.

## 100k state-key cardinality estimate

A read-only `StaticFileProvider` pass over Base blocks
**51,398,021..51,498,020** streamed 19,866,464 account rows and 88,534,816 storage
rows into HyperLogLog counters. It estimated approximately **940,971 unique account
addresses** and **17,339,777 unique `(address, slot)` keys**. HLL results are
approximate cardinalities, not retained exact keys or semantically complete forward
state. Summary SHA-256:
`e00f51a7cc9be49a2560342c20df44f15bc8dd2a53285eab1bb84042f2fccd8a`.

The corrected generator retains the measured 1,000-block local pools (20,614 accounts,
244,196 storage keys) and slides them deterministically so the 100-epoch unions equal
the HLL estimates.

## Corrected sealed-epoch 100k result

The corrected Base-cardinality-shaped generator completed all 100 sealed epochs in
**228.7859 seconds**, producing **17,456 immutable objects** and **1,141,404,991
logical immutable bytes**. It passed the prototype gates of 600 seconds, 35,000
objects, and 2.75 GB. The compact checked result is
`benchmarks/results/v2-base-shaped-epoch-100k.json`; the complete progress result has
SHA-256 `a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.

This run used the filesystem backend on the devbox and the approximate HLL cardinality
shape. It is not production R2 evidence or semantically complete forward EVM state.

## Rejected high-cardinality tuned run

A tuned-format 100k run completed in **425.3685 seconds** with **17,456 objects** but
used **3,006,815,546 bytes**. Its generator used 2.1 million account and 24.4 million
slot pools and made almost every epoch key unique, so it is intentionally rejected as
a pessimistic high-cardinality generator result and is not Base-shaped.

## First sealed-epoch 100k tuning result

The first exact sealed-epoch run completed in 505.440 seconds but produced 41,009
immutable objects and 3,297,864,788 bytes: 6,400 data, 25,600 index, 8,706 checkpoint,
101 directory, 2 catalog, and 100 commit objects. It passed the runtime target but
failed the 35,000-object and 2.75 GB targets. The retained result SHA-256 is
`ab028cce9f88858725300fa05b82853cbeb037b3d9cb5820778dbd76774cbd93`.
This motivated 128 router partitions, 32 data partitions, 8 checkpoint subshards, and
one closing checkpoint. Those superseded runs remain rejected tuning evidence.

## Rejected state-key COW scale run

The exact 100,000-block run using the former immutable state-key COW prototype emitted
929,134 files (about 3 GB logical/5 GB allocated) before exhausting the 1,048,576
`/tmp` inode limit. A rerun on md0 accumulated 26 GB and was unfinished after 15
minutes. This is negative design evidence explaining the sealed-epoch pivot, not a
current-v2 result.

## Caveats

Reth state changesets are before/unwind values, not a semantically complete forward
state stream. These measurements establish changeset shape and physical-layout codec
evidence only. They do not include a Fossil anchor, final post-state reconstruction,
bytecode completeness, incarnation semantics, sealed-epoch publication, RPC serving,
or production object-store behavior. The checked epoch benchmark is synthetic and
lists retained real measurements separately. No giant raw fixture is checked in.
