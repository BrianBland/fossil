# Limitations

- V2 is a focused sealed-epoch prototype, not a production archive-node replacement.
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
- Readers have a bounded verified in-process immutable-object cache but no persistent
  local index/cache. Correctness and readiness do not depend on cache contents. Each
  logical lookup has internal object-GET/decoded-byte caps, but process-wide concurrent
  miss control remains an operator deployment limit.
- V2 refresh verifies a newer head/commit and accepts skipped generations only when
  chain/genesis/anchor identity matches and number/generation advance. Lazy object
  corruption is discovered on access. V1 rejects refresh. `verify --archive-format
  v2` checks head/commit only; no full-history audit exists.
- Code CAS objects have a 1 MiB prototype hard limit; chains requiring a larger
  contract-code policy need a format revision and new resource measurements.
- V2 EIP-1898 block-hash selectors are rejected until a durable bounded hash-to-number
  index exists; numeric and tag selectors remain supported.
- Fixed offset is policy, never protocol finality. Finalized checkpoints are trusted
  external input.
- State epochs, index deltas, checkpoints, completed directories/catalogs, and code
  are durable. No automatic GC, provider inventory, deletion workflow, signature,
  rollback tool, or disaster-recovery tool is included. Superseded active directories,
  commits, and orphans require future offline GC after a rollback window.
- Memory/filesystem and generic conditional object-store behavior are tested locally.
  Real R2, AWS S3, and MinIO conditional-write/latency tests remain required.
- Workers are only a future stateless serving target. There is no Worker deployment,
  log/call serving code, or Durable Object bulk path.
- V1 remains supported and frozen with its original eager indexes and segment scans.
  V2 uses a separate epoch head and never falls back across formats.
