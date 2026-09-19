# Limitations

- Fossil is a focused sealed-epoch prototype, not a production archive-node replacement.
  No direct execution-client exporter or version-pinned chain compatibility suite is
  included.
- Supplied flat values/state roots are retained, but trie proofs and independent
  Ethereum state-root reconstruction are absent. Hashes prove object integrity, not
  publisher authenticity, consensus, or semantic correctness. Exporter/checkpoint
  values remain authoritative trusted input; Fossil rejects cross-object and checkpoint
  time inconsistencies but does not prove Ethereum state roots.
- `eth_getProof`, blocks, transactions, receipts, logs, filters, subscriptions,
  tracing, and EVM execution/`eth_call` remain unsupported. Their bounded deferred
  designs are documented separately.
- The checked 1,000-block workload is synthetic changeset-shape evidence. The external
  real 100,000-block result is codec/layout evidence, not end-to-end epoch publication
  or forward-state validation.
- The corrected Base-cardinality-shaped 100,000-block run passed its prototype gates
  in 228.786 seconds with 17,456 objects and 1,141,404,991 bytes. It remains synthetic
  physical-layout/publication evidence: it does not contain authoritative forward state,
  a production exhaustive anchor, code traffic, provider latency/retries, or R2 billing.
- The completed Base-cardinality-shaped month stress test sealed 1,296,000 blocks in
  9,449.0865 seconds with 234,368 objects and 23,277,861,710 logical bytes. Its
  [checked artifact](../benchmarks/results/base-shaped-month.json) is filesystem-backed
  synthetic evidence, not a publication of the measured real Base range, semantically
  complete forward EVM state, RPC-serving evidence, or production R2 performance or
  billing. The approximately $0.349/month figure assumes $0.015 per decimal GB-month
  and covers storage only.
- Readers have a bounded verified in-process immutable-object cache but no persistent
  local index/cache. Correctness and readiness do not depend on cache contents. Each
  logical lookup has internal object-GET/decoded-byte caps, but process-wide concurrent
  miss control remains an operator deployment limit.
- Refresh verifies a newer head/commit and accepts skipped generations only when
  chain/genesis/anchor identity matches and number/generation advance. Lazy object
  corruption is discovered on access. `verify` checks head/commit only; no
  full-history audit exists.
- Code CAS objects have a 1 MiB prototype hard limit; chains requiring a larger
  contract-code policy need a format revision and new resource measurements.
- Fossil EIP-1898 block-hash selectors are rejected until a durable bounded hash-to-number
  index exists; numeric and tag selectors remain supported.
- Fixed offset is policy, never protocol finality. Finalized checkpoints are trusted
  external input.
- State epochs, index deltas, checkpoints, completed directories/catalogs, and code
  are durable. No automatic GC, provider inventory, deletion workflow, signature,
  rollback tool, or disaster-recovery tool is included. Superseded active directories,
  commits, and orphans require future offline GC after a rollback window.
- Memory/filesystem and generic conditional object-store behavior are tested locally.
  Real R2, AWS S3, and MinIO conditional-write/latency tests remain required.
- The public, read-only Rust/WASM Cloudflare Worker implements bounded active-window
  balance, nonce, code, and storage lookups plus chain ID, block number, client version,
  and an immutable-object gateway under `fossil-demo/`. It does not implement completed
  windows, checkpoint fallback, logs, calls, tracing, writes, deletes, or listing. The
  native Tokio/Axum adapter remains the full JSON-RPC server. See the sparse
  [live demo](live-demo.md).
