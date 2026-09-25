# How Fossil stores history: a visual guide

This page is the picture-first companion to the [format specification](format.md).

## 1. The big picture

A Base node replays finalized blocks. The exporter turns each 1,000-block package into an
immutable **run**. A compactor merges runs in the background. One small mutable **head**
says which runs are current. Readers only ever follow the head.

```mermaid
flowchart LR
    node[(Base node)] -->|replay| exporter[exporter]
    exporter -->|fossil archive| bucket
    compactor[fossil compact] <-->|merge runs| bucket
    gc[fossil gc] -->|delete unreachable| bucket
    subgraph bucket [R2 / S3 bucket]
        head[/"head: tiered-v1.json"/]
        objects[(immutable objects)]
    end
    head --> objects
    bucket --> server[native RPC server]
    bucket --> worker[Cloudflare Worker]
```

## 2. What a run contains

A run holds every state version written during its block range, keyed by
`(key, block)`. Keys are `0x01 ‖ address` for accounts, `0x02 ‖ address ‖ incarnation ‖
slot` for storage, and `0x03 ‖ code hash` for code.

```mermaid
flowchart TD
    root["run root (581 bytes)"] --> p0["partition 0 fence tree"]
    root --> pdots["…"]
    root --> p15["partition 15 fence tree"]
    p0 --> leaf["fence leaf ≤512 KiB<br/>first (key, block) of each page"]
    leaf --> d1["data page ≤1 MiB<br/>sorted (key, block, value)"]
    leaf --> d2["data page"]
    root -.->|"same run, keyed by root digest"| sum["summary shards<br/>(Bloom filters)"]
```

- Keys are spread over 16 partitions by hash, so no single object grows without bound.
- A **fence tree** finds the one data page that can hold `key` at or before a block.
- **Summary shards** answer “might this run contain this key?” with no false negatives.
  An account and all of its storage share one shard, so one download covers both.

## 3. The head is the whole table of contents

```mermaid
flowchart LR
    head[/"head (JSON, ≤256 KiB)"/] --> L2["level 2 run<br/>blocks 0 – 2,047,000"]
    head --> L1["level 1 run<br/>blocks 2,047,001 – 3,071,000"]
    head --> L0a["L0 run<br/>blocks 3,071,001 – 3,072,000"]
    head --> L0b["L0 run<br/>3,072,001 – 3,073,000"]
    head -. parent .-> old[/"previous head (immutable copy)"/]
```

Runs never overlap in time. Older history lives in a few large runs; recent history
lives in small ones. Because the run list is inline, a reader needs **one GET** to know
everything it will look at.

## 4. Reading `eth_getStorageAt(address, slot, block)`

```mermaid
sequenceDiagram
    participant R as reader
    participant S as bucket
    R->>S: GET head
    loop each run, newest first, starting at or before block
        R->>S: GET summary shard for address
        alt key absent
            Note over R: skip this run
        else might be present
            R->>S: GET fence root / leaf
            R->>S: GET data page
            Note over R: exact match ≤ block? done
        end
    end
    Note over R: nothing found → zero / absent
```

Storage first resolves the account at that block (for its incarnation), then the slot;
the summary shard fetched for the account is reused for the slot. Cold cost is roughly
**1 + (runs) + 6 GETs**, with a hard cap of 192 GETs and 128 MiB per request.

## 5. Writing: append now, merge later

```mermaid
flowchart LR
    pkg[package] -->|build + upload objects| newrun[new L0 run]
    newrun -->|CAS head: append| h1[/"head"/]
    h1 -->|4 L0 runs waiting| merge[compactor: one streaming merge]
    merge -->|upload merged run| lvl[level run]
    lvl -->|CAS head: replace inputs| h2[/"head"/]
```

- Publishing only **appends** a run and swaps the head with compare-and-swap. It never
  waits for merging.
- The compactor folds four L0 runs (and any full level they fill) into one bigger run.
  Level `i` holds up to `4 × 8^i … 4 × 8^(i+1)` epochs, so the run count stays small.
- Either writer can lose a CAS race; it re-reads the head and retries. A reader always
  sees either the old complete head or the new one, never a partial run.
- If 16 L0 runs pile up, publishing pauses until the compactor catches up.

## 6. Cleaning up

Merged inputs stay in the bucket until `fossil gc` removes them:

```mermaid
flowchart LR
    head[/"head"/] --> live["reachable: current runs,<br/>their pages and shards,<br/>recent head copies"]
    all[(all objects)] --> check{reachable?}
    check -->|yes| keep[keep]
    check -->|no, and older than 3 h| drop[delete]
    check -->|no, but recent| keep
```

GC runs safely next to the exporter and compactor: new uploads are protected by the
3-hour grace period, and writers re-stamp any old object they reuse.
