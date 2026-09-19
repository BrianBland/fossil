# Consistency and publication

`finalized` accepts a trusted number/hash. `fixed-offset` verifies an observed tip and
publishes through `tip - offset`; it is operational lag, not protocol finality.
Conflicting hashes, gaps, chain/genesis mismatch, or a delta not immediately following
the committed head fail closed. Identical anchor/delta retries validate their
checkpoint and return the already-published commit before transition validation, so a
CAS loser can retry idempotently.

## Sealed head-last protocol

V1 retains its frozen segment protocol. V2 publication:

1. reads and verifies the epoch head and fixed commit;
2. validates input, continuity, gate, and exact code references;
3. locally groups the complete next bounded epoch by key and block version;
4. uploads create-only data and compact index-delta partitions;
5. writes one new active-window directory; after epoch 64 it builds one closing
   checkpoint partition-by-partition, emits bounded subshards/manifests, the completed
   directory, and one two-level numeric catalog update;
6. uploads the fixed-size commit; and
7. rechecks the gate and CAS-replaces the mutable epoch head.

Only step 7 changes visibility. An open epoch is never referenced by a head. A failed
or interrupted writer can leave unreachable immutable objects but cannot expose a
partial epoch. Publication never deletes objects.

V2 readiness verifies only head and commit. Directories and state objects remain lazy.
The fixed-size v2 head is read through a hard streaming bound. When fetched, the active
directory start and completed catalog ranges must match committed metadata exactly;
a nonempty active directory must end at the published block, while an empty rollover
directory must start exactly one checked block after it.
Every request uses one immutable publication snapshot. Refresh ignores an equal head,
rejects older or unrelated chain/genesis/anchor identity, and accepts any strictly
newer verified authoritative head even if multiple generations were skipped between
polls. Audit parents are validated but not traversed. Any refresh failure retains the
old snapshot; v1 does not support refresh.

Filesystem CAS uses a process-independent lock and atomic rename. S3-compatible CAS
uses ETag only as an opaque version token. Provider conditional-write behavior still
requires deployment-specific smoke testing.
