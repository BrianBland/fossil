# Consistency and publication

## Gates

`finalized` accepts an explicit trusted number/hash and only publishes through that
exact canonical checkpoint. `fixed-offset` verifies an observed input tip and
publishes through `tip.saturating_sub(offset)`. The latter is an operational lag
policy, not finality. Consequently, RPC `safe` and `finalized` selectors are rejected
for fixed-offset publications; only `latest` names that policy head. Fossil inherits
trust in the source that labels a finalized checkpoint; it does not run consensus.

Any different hash at or below the committed head, wrong parent, gap, chain/genesis
mismatch, or delta not immediately following the head is fatal. Existing immutable
history is never rewritten. Identical content upload and publication retry are
idempotent.

## Manifest-last protocol

1. Read and retain the current head version.
2. Validate input ordering, chain continuity, code, exact gate hash, and previous head.
3. deterministically encode and hash segment/index objects.
4. Upload immutable objects with create-only semantics; an existing object is accepted
   only when its bytes match.
5. Upload the complete content-addressed manifest.
6. Recheck the gate and conditionally create/update the mutable head against the
   retained version.

Only step 6 changes reader visibility. A competing publisher causes the conditional
operation to fail. Orphans from an interruption are harmless and deliberately not
deleted. Filesystem CAS is guarded by a process-independent lock and atomic rename;
S3 CAS uses the provider ETag solely as an opaque version token.

A server verifies the committed manifest, mirrors every index, and only then becomes
ready. Each HTTP request/batch uses one immutable in-process publication. This
prototype loads a new generation on server restart; automatic background head polling
and atomic publication swaps remain follow-up work.
