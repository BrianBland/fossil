# Live Base R2/Cloudflare Worker demo

A public, read-only Rust/WASM Cloudflare Worker is serving a Fossil format-v1 demo
archive directly from R2:

- JSON-RPC endpoint: <https://fossil-r2-gateway.brian-t-bland.workers.dev/rpc>
- R2 bucket: `base-mainnet-fossil`
- archive prefix: `fossil-demo/`
- chain ID: `8453` (`0x2105`)
- published generation: `1`
- published commit:
  `0x8bda03b969bc1361cd7af354dbbdaafff8478a7e51c2d9491a1193c72495d95f`

The endpoint accepts only the [standard methods implemented by the
Worker](../worker/README.md#api-and-limits). It has no write, delete, or list API and
requires no credentials. Do not put R2 credentials in commands or URLs.

## Copy-paste verification

The following checks require [Foundry `cast`](https://getfoundry.sh/cast/overview) and
`jq`. They use standard Ethereum JSON-RPC method names rather than Fossil-specific
methods.

```sh
export FOSSIL_RPC='https://fossil-r2-gateway.brian-t-bland.workers.dev/rpc'
export DEMO_ADDRESS='0x0000000071727de22e5e9d8baf0edac6f37da032'
export DEMO_STORAGE_SLOT='0x249066a61b5655ddbe7b9f1de916ebd81e16cc0239c87e0643fa09b867ec4459'
export DEMO_BLOCK='0x3123592'

cast rpc --rpc-url "$FOSSIL_RPC" web3_clientVersion
# "fossil-worker-rs/0.1.0"

cast rpc --rpc-url "$FOSSIL_RPC" eth_chainId
# "0x2105"

cast rpc --rpc-url "$FOSSIL_RPC" eth_blockNumber
# "0x3123592"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$DEMO_ADDRESS" "$DEMO_BLOCK"
# "0x209c814eaf02101ed"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getTransactionCount "$DEMO_ADDRESS" "$DEMO_BLOCK"
# "0x2"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt \
  "$DEMO_ADDRESS" "$DEMO_STORAGE_SLOT" "$DEMO_BLOCK"
# "0x0000000000000000000000000000000000000000000000000000000000000001"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getCode "$DEMO_ADDRESS" "$DEMO_BLOCK" \
  | jq -r '"bytes: \((length - 2) / 2)\nprefix: \(.[0:66])"'
# bytes: 16035
# prefix: 0x60806040526004361015610024575b361561001957600080fd5b610022336127
```

Block `0x3123592` is decimal `51,525,010`. Fossil values are post-execution state for
the selected block.

## Dataset scope and limitations

This demo covers the inclusive Base range `51,525,001..51,525,010`
(`0x3123589..0x3123592`). It was assembled from real Reth changeset keys plus local
RPC post-state. It deliberately contains a **sparse, non-exhaustive anchor** for only
the observed changed keys and incarnation `0`; it is not a production-complete Base
archive or an exhaustive state snapshot. The demo validates archive generation,
format decoding, integrity checks, R2 access, and standard JSON-RPC serving. It must
not be used to infer results for arbitrary accounts, slots, incarnations, or blocks
outside that narrow range.

At publication, `fossil-demo/` contained 640 objects totaling 5,294,478 bytes
(5.049 MiB); the inventory was complete, not truncated. A separate 1 GiB R2 transport
upload smoke test completed in 27.699 seconds (about 38.76 MB/s). That transport blob
was then deleted, leaving the bucket total below 1 GB. The smoke result is a single
upload observation, not Worker query throughput or a production performance claim.
