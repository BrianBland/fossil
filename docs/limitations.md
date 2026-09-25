# Limitations

- Fossil is a focused archive prototype, not a production archive-node replacement.
  The [Base exporter](../tools/base-export/README.md) is version-pinned to one Reth
  datadir layout and has no multi-chain suite.
- Trie proofs and independent Ethereum state-root reconstruction are absent. Hashes
  prove object integrity, not publisher authenticity or semantic correctness. The
  exporter is trusted for cross-package account lifetimes: publication checks
  incarnations only where the package itself contains the account record, and checks
  code availability only for the anchor.
- `eth_getProof`, blocks, transactions, receipts, logs, filters, subscriptions,
  tracing, block-hash selectors and `eth_call` are unsupported.
- The cold-read target (p99 at most 20 GETs for account plus storage) holds in steady
  state (three L0 runs plus one run per level), but a full 16-run compaction backlog,
  addresses whose keys spill out of their address shard, and Bloom false positives add
  GETs. Real-traffic p99 must be measured with `fossil probe` on a populated archive.
- Summary construction keeps every shard of a run in memory (about four bytes per
  expected key), which bounds the largest mergeable run by compactor RAM.
- No garbage collection exists. Superseded compaction inputs and head copies remain
  in the bucket and count toward any storage cap.
- Code blobs are limited to 1 MiB and single records to one 1 MiB page.
- Fixed offset is policy, never protocol finality.
