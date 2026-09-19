# Federated archive roadmap

The MVP intentionally keeps immutable object identity separate from storage location.
Manifests refer only to SHA-256 content IDs and exact lengths, while the archive/query
layers use the narrow `ArchiveStore` interface. A future Portal-Network-like backend
could therefore resolve one immutable object from several peers without changing its
identity or trusted decoded result.

A horizontally scaled experiment could assign full or fractional content-ID ranges to
peers. Each peer could use memory, local disk, Redis, S3/R2, or a tiered combination as
its private storage implementation. Peers would exchange content-addressed
segment/index/manifest bytes; the existing length and digest verification would run
before caching or parsing. Manifests provide a common verifiable catalog, and the
`fossil_archive_segments` / `fossil_archive_object_bytes` metrics provide the basic
inputs for simulating fractional per-peer capacity.

This is only a format/backend seam, not a distributed system implementation. The MVP
deliberately defers:

- DHT routing and wire protocols;
- peer discovery and identity;
- replication, repair, and placement policy;
- incentives, accounting, and abuse resistance;
- publisher authentication and manifest consensus;
- availability, durability, latency, and data-retention guarantees.

A future design must preserve manifest-last canonical publication and fail closed when
no peer returns bytes matching the requested immutable identity. The mutable canonical
head also needs an authenticated distribution mechanism; content addressing alone does
not decide which manifest is canonical.
