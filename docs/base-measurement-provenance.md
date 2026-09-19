# Base measurement provenance

These retained summaries were sampled on **2026-09-19**. The 100,000-block evidence
ends at finalized Base head **51,498,020**; the month evidence ends at finalized Base
head **51,525,110**. The machine was `devbox` with **32 cores, 128 GiB RAM, and a
15 TiB RAID**. The node image was `ghcr.io/base/node:v1.3.0-rc.6`; its Base Reth source was
tag `base-v2.5.2.6`, commit `5877708b`.

## Extraction methodology

Block and receipt shape came from canonical RPC block/receipt responses at the sampled
finalized head. State changeset shape and physical sizes came from a read-only Reth
`StaticFileProvider` using the state changeset offset/data files (`csoff`/`off`). No
node database was mutated. The 100,000-block layout run processed 100 real 1,000-block
chunks and discarded each decoded chunk after encoding its layout outputs. The month
layout pass processed 1,296 real 1,000-block chunks in five resumable slices; its
reported elapsed time is the sum of those slices.

The retained summaries and full month progress result have these SHA-256 digests:

| Summary | SHA-256 |
|---|---|
| `base-1000-rpc` | `72ce7fc51f252c4d0bcda1b624a092f06f9ab7d91043962afd97448e8ef63698` |
| `base-1000-state` | `2bae2a63e1864193fca2b1bf90fae2947e387766a66dc903faa65e156e96f034` |
| codec sizes | `866a8ff20d4baf36135c5d8ce9795c84e58288557023c047b2930db0cfa8951a` |
| codec timing | `0017d8d0ca6939742280b66e3d5d5644cab2410a628145fe4bf618a2f67cc957` |
| 100k layout | `d35bc9aaf183537f332a6687089f349060d21d9650193e719c196b7438b7f01b` |
| month HLL | `42c965456b49243378b4c106c99ee6be50a40611adff5bf8232e1ab3bf5b511e` |
| month Reth physical changesets | `800390687bcea3ebc1b24dc50589b1708d246af5550b23a561234b4a1b5a1011` |
| month layout | `15fb23b5244a74c3896cfc2cf6aa51a06e1470d0f28b11daf286981cbea2434c` |
| month sealed-epoch full result | `c72c2960789cbf6000c42624dacaab03d70939a239747149dc3825d901c977c4` |

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

## Month cardinality, physical size, and layout evidence

A second read-only `StaticFileProvider` pass covered finalized Base blocks
**50,229,111..51,525,110** (1,296,000 blocks, approximately 30 days at a two-second
block time). It streamed **277,758,377 account rows** and **1,154,636,022 storage
rows**. HyperLogLog estimated **7,956,411 unique accounts** and **218,828,772 unique
`(address, slot)` keys**. These are approximate cardinalities, not retained exact key
sets. HLL summary SHA-256:
`42c965456b49243378b4c106c99ee6be50a40611adff5bf8232e1ab3bf5b511e`.

Reth changeset data plus offsets occupied **93,831,464,168 bytes**:
**11,759,303,278 account bytes** and **82,072,160,890 storage bytes**. Physical-size
summary SHA-256:
`800390687bcea3ebc1b24dc50589b1708d246af5550b23a561234b4a1b5a1011`.

Layout-only processing of all 1,296 real 1,000-block chunks took **814.453 seconds**
summed across five resumable slices. Block-major Zstd-9 used **24,476,613,098 bytes**;
key-major Zstd-9 used **20,030,070,751 bytes**, or **15,455.30 bytes/block** and
**18.1665% less** than block-major. Layout summary SHA-256:
`15fb23b5244a74c3896cfc2cf6aa51a06e1470d0f28b11daf286981cbea2434c`.
This is real changeset **layout-only** evidence, not an end-to-end Fossil publication,
checkpoint/catalog, serving, or production object-store result.

The synthetic generator retains the measured 1,000-block local pools (20,614 accounts,
244,196 storage keys). Its deterministic sliding offsets interpolate first from epoch
0 to epoch 99 so the 100-epoch unions equal 940,971/17,339,777, then from epoch 99 to
epoch 1,295 so the month unions equal 7,956,411/218,828,772. Consecutive epoch pools
overlap and generated IDs remain inside the month universes.

## Completed sealed-epoch month result

The deterministic Base-cardinality-shaped stress test completed **1,296,000 blocks**
and all **1,296 1,000-block epochs** in **9,449.0865 seconds (2h37m29s)** on the
documented devbox. It produced 20 completed windows plus 16 active epochs,
**234,368 immutable objects**, and **23,277,861,710 logical immutable bytes**. The
object counts include 41,472 data, 165,888 index, 23,060 checkpoint, 1,316 directory,
40 catalog, and 1,296 commit objects. The compact checked result is
[`../benchmarks/results/base-shaped-month.json`](../benchmarks/results/base-shaped-month.json);
the full progress result has SHA-256
`c72c2960789cbf6000c42624dacaab03d70939a239747149dc3825d901c977c4`.

This result is a completed filesystem-backed sealed-epoch physical-layout stress test,
not a Fossil archive built from authoritative forward Base state and not production R2
performance. It uses measured change counts and approximate HLL cardinalities, but it
excludes semantically complete forward state, code traffic, a production exhaustive
anchor, provider latency/retries, and provider billing.

## Corrected sealed-epoch 100k result

The corrected Base-cardinality-shaped generator completed all 100 sealed epochs in
**228.7859 seconds**, producing **17,456 immutable objects** and **1,141,404,991
logical immutable bytes**. It passed the prototype gates of 600 seconds, 35,000
objects, and 2.75 GB. The compact checked result is
`benchmarks/results/base-shaped-epoch-100k.json`; the complete progress result has
SHA-256 `a3ac0318a50c06617b43d93f16e9487875f8e22f3d9683f450d7f357d7bd3fc4`.

This run used the filesystem backend on the devbox and the approximate HLL cardinality
shape. It is not production R2 evidence or semantically complete forward EVM state.
It predates the month-universe storage-address mapping introduced with the piecewise
pool formula, so it remains historical measured evidence rather than a claimed result
from the current generator without a new 100k run.

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
minutes. This is negative design evidence for choosing sealed epochs, not a result for the
implemented format.

## Caveats

Reth state changesets are before/unwind values, not a semantically complete forward
state stream. The real-range measurements establish changeset shape and layout-codec
evidence only; they do not provide a Fossil anchor, final post-state reconstruction,
bytecode completeness, or incarnation semantics. The completed month sealed-epoch
benchmark is synthetic filesystem evidence shaped by those measurements, not a
publication of that real Base range, RPC-serving evidence, or production object-store
behavior. No giant raw fixture is checked in.
