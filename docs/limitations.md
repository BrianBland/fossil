# Prototype limitations

- There is no direct execution-client database reader. No client or chain version
  compatibility is claimed; normalized exporters and version-pinned conformance
  fixtures are follow-ups. Reth/ExEx/MDBX is one possible adapter, not a core format
  dependency.
- Flat values and a supplied state root are stored, but trie nodes, proofs, and
  independent state-root reconstruction are absent. Checksums prove integrity, not
  Ethereum correctness, authenticity, or consensus.
- `eth_getProof`, calls, EVM execution, tracing, blocks, transactions, receipts, logs,
  filters, and subscriptions are unsupported and return method-not-found.
- Fixed offset is probabilistic policy, never finality. Finalized checkpoints are
  trusted external input.
- A whole input package becomes one zstd segment. Serving fetches whole objects rather
  than authenticated ranges; large production exports need deterministic frame
  splitting, sparse routing structures, and measured compaction.
- The disk cache is verified and persistent but has no byte-budget eviction in this
  pass. The memory cache is bounded. Indexes are pinned by remaining on disk.
- Routing indexes are held in memory rather than a rebuildable `redb` database.
  Startup mirrors them before readiness; very large catalogs require a disk-backed
  router.
- A running server does not poll for new heads. Restart loads the next complete
  generation. There is no multi-region cache coordination.
- The full catalog is copied into each manifest and lookup scans segment-level index
  catalogs newest-first. Production EVM-chain scale requires checkpoint/delta manifests
  and an immutable B-tree/LSM router.
- No garbage collector, object deletion, publisher signatures, disaster-recovery
  tooling, or mutable-head rollback exists. Configure bucket versioning, retention,
  backups, and deletion permissions operationally.
- Memory/filesystem behavior and the generic conditional object-store adapter are
  tested in CI. Real AWS S3, R2, and MinIO provider smoke tests are not included, so
  production interoperability is not yet claimed.
- The S3 path uses provider conditional requests but cannot prove their contract at
  startup without a write. Validate the exact provider/client combination before use.
