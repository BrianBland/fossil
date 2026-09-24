# Consistency and publication

`finalized` accepts a trusted number/hash. `fixed-offset` with offset zero accepts an
observed canonical tip. Conflicting hashes, gaps, chain/genesis mismatch, or a delta
not immediately following the committed head fail closed. An identical retry of the
package the head already records returns success without writing.

## Head-last protocol

Publication:

1. reads and fully validates the current head;
2. validates the package's block interval, gate and in-package account/storage
   lifecycle (every anchor code reference must be present);
3. builds the run's pages, fence tree, root and summary shards and uploads them
   create-only (identical bytes are accepted, different bytes fail);
4. CAS-replaces the head with one whose L0 list gains the new run and whose `parent`
   references an immutable copy of the head it replaced.

If the CAS loses to a compactor that replaced runs under the same chain head, the
publisher rebases onto the new head and retries; any other change fails. Compaction
plans a single k-way merge from one head, uploads the merged run, then CAS-replaces the
head only if its levels are unchanged and its L0 list still begins with the merged
runs, preserving any runs appended meanwhile. Only a head CAS changes visibility; a
failed writer can leave unreachable objects but never a partial run.

Readers pin one head per request (the native server swaps in newer verified heads at
its refresh interval, never an older block). Filesystem CAS uses a process-independent
lock and atomic rename; S3-compatible CAS uses the ETag as an opaque version.
