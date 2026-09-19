# Historical `eth_call` design (explicitly deferred)

Historical EVM execution is not implemented. Work must not begin until v2 exact state
reads, block selection, and the complete block/chain execution context have been
validated against an independent client.

An `eth_call` needs more than account and slot values: the exact hardfork rules,
chain configuration, block header fields, ancestor block hashes, transaction/call
environment, fee fields, precompiles, and state override semantics must all match the
selected chain and block. Returning a plausible result with incomplete context is
worse than returning method-not-found.

Execution produces dynamic, sequential state misses. A contract can discover another
address or slot from prior bytecode/data, so the full read set cannot generally be
prefetched. A future executor should issue verified v2 exact/predecessor reads through
a request-local cache, backed by a bounded shared state-object/code cache. It can speculatively
prefetch only evidence-based neighbors and must preserve deterministic error and gas
semantics. Edge caches may retain immutable epoch objects by digest; no cache is authoritative.

Two deployment shapes need separate measurement:

- **Thin regional servers:** a native EVM, larger bounded memory cache, connection
  pooling, and optional verified filesystem cache. This is the likely default for
  long or miss-heavy calls.
- **Workers/edge isolates:** useful for short calls when required immutable objects are
  already near the edge, but constrained by CPU duration, subrequest count, memory,
  code size, and streaming limits. Dynamic sequential misses can dominate latency.

Both remain disposable readers over the same centralized immutable backend. Durable
Objects are not needed for bulk storage or the read path.

## Mandatory execution caps

Any future implementation must meter and cap every call independently. Initial limits
must cover supplied gas, wall-clock execution time, state-object GETs, code-object GETs,
transferred encoded bytes, decompressed bytes, request-local cache bytes, encoded
response bytes, and concurrent executions per process. The request-local cache must be
bounded and discarded after the call; a shared immutable cache cannot substitute for
its budget. A suggested conservative validation envelope is 30 million gas, 5 seconds,
256 state-object GETs, 64 code GETs, 64 MiB transferred, 128 MiB decompressed/request-local
cache, 16 MiB response, and a configured semaphore no larger than available execution
cores.

Crossing any cap must stop execution and return a stable deterministic resource-limit
error identifying the exhausted budget. It must not return partial output, silently
increase gas, or vary based on whether an immutable object happened to be cached.
Timeout and concurrency rejection semantics need differential and load tests.

Before enabling the method, differential tests must cover all active hardfork
transitions, environmental opcodes, block-hash lookback, precompiles, revert/error
payloads, state overrides, access-list behavior, every resource limit, and first-pass
versus cached object-request bounds.
