# Deployment and runtime architecture

Fossil has one archive format and two serving adapters at different implementation
levels. It does **not** have two archives.

## Native server: current implementation

The current `fossil serve` binary uses Tokio and Axum. HTTP connections and remote
R2/S3 object-store requests are concurrent network I/O workloads: most time is spent
waiting for sockets rather than consuming a CPU core. Tokio provides a natural bounded
async model for those waits, and Axum exposes the JSON-RPC, health, and metrics routes.

The filesystem backend is an important exception: it currently calls synchronous
`std::fs` operations inside async store methods. That path is suitable for prototype
validation, but production local-SSD serving must move filesystem reads/writes into
`spawn_blocking` or a dedicated blocking pool, or use a measured async filesystem
adapter, so disk waits do not block Tokio I/O workers.

A production native deployment should enforce:

- bounded request and batch concurrency;
- bounded object-store GET concurrency in addition to each lookup's hard GET/decoded
  byte limits;
- a bounded verified-object cache with per-object singleflight, so concurrent misses
  for one digest perform one backend load;
- client, provider, and whole-request deadlines;
- a separate blocking/CPU pool or semaphore for synchronous filesystem access, future
  EVM execution, tracing, Zstd work that proves CPU-heavy, or other non-async
  computation; and
- process memory limits sized above cache capacity and bounded in-flight decoding.

Do not run CPU-heavy future `eth_call` execution directly on Tokio I/O workers.
Native regional servers are the preferred tier for miss-heavy historical `eth_call`,
tracing, large working sets, or deployments that benefit from a local SSD cache.
Historical EVM execution and tracing are not implemented today.

## Cloudflare Worker: Rust/WASM active-window reader

[`worker/`](../worker/README.md) is a worker-rs crate compiled to WASM. Its direct R2
binding loads the normal mutable publication head and immutable format-v1 objects; the
generated JavaScript is only a loader, not a second archive decoder. The same
platform-neutral Rust core is exercised natively against the checked deterministic
Fossil fixture.

The Worker implements bounded Ethereum JSON-RPC balance, nonce, code, storage, chain
ID, and block-number reads, plus the standard client version method. It also retains
a read-only streaming gateway restricted to canonical
`/objects/objects/sha256/<shard>/<digest>` CAS keys, with byte ranges and ETags. Mutable
heads are reachable only by RPC internals. There are no list or write operations and no
custom `fossil_*` RPC methods. A sparse,
public, read-only deployment and exact verification commands are documented in the
[live Base R2/Cloudflare Worker demo](live-demo.md).

The edge implementation is deliberately active-window-only. It rejects completed
windows and checkpoint fallback explicitly instead of returning a false default. The
native server remains the full reader for those paths and is also the preferred tier
for miss-heavy workloads, broad working sets, future EVM execution, and tracing.
Neither runtime currently implements historical `eth_call`, log filtering, or tracing.

```text
one format-v1 archive + shared golden object graph
    |-- native fossil binary (Tokio/Axum/object_store)
    `-- Rust/WASM Worker (worker-rs/direct R2/active window)
```

Worker limits vary by plan and release. The implementation enforces a 16 KiB RPC body,
192 R2 GETs, 128 MiB aggregate fetched and decoded, an 8 MiB decoded/8.25 MiB encoded
Worker data-object cap, a 2 MiB encoded index cap with bounded filtered candidates,
pre-I/O ObjectRef budget reservation, and an 8 MiB per-request object cache. The native
format retains its separate 64 MiB data-object allowance.
These are safety ceilings, not proof that a worst-case lookup fits a particular
Cloudflare CPU/subrequest plan; production deployment must measure
real archive paths and may need lower method-specific limits. See the Worker README
for the pinned Rust/WASM toolchain, dry-run bundle check, direct Wrangler deployment,
RPC examples, and current limitations.
