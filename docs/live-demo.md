# Live Base genesis R2/Cloudflare Worker demo

The canonical Rust/WASM Cloudflare Worker deployment serves a Fossil format-v1
archive directly from R2:

- endpoint: set `$FOSSIL_RPC` to `https://YOUR-WORKER.workers.dev/rpc`;
- R2 bucket: `fossil`;
- archive root: `v1/`;
- chain: Base, chain ID `8453` (`0x2105`); and
- complete usable state range: **block `0..0`**.

The endpoint remains a placeholder here deliberately. Do not put R2 credentials in
client commands or URLs. The Worker is read-only and exposes no write, delete, or list
operation.

## Exhaustive block-0 anchor

This is an exhaustive Base genesis anchor, not a sparse key overlay. The block-0
package includes every account in the Base genesis allocation, every nonzero genesis
storage slot, and every unique referenced code blob:

| Property | Value |
| --- | ---: |
| Block hash | `0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd` |
| State root | `0xb2afcb88cd1d0ab228f0415d99b0fb90a18e8515daf5eb31f55b5c4697e18328` |
| Accounts | 2,064 |
| Nonzero storage slots | 2,075 |
| Unique code blobs | 16 |
| Source package size | 1,181,558 bytes |
| Local archive commit | `0x03bb43f7841f56460e8214d2e8900b97cb26ae802fa9d529ea52f7af645e7470` |

Ethereum zero/absence semantics apply to accounts and slots not present in that
exhaustive allocation. Fossil retains the authoritative block hash and state root, but
it does not independently reconstruct the Ethereum trie; as elsewhere in the format,
the exporter and anchor package are trusted inputs.

The live `v1/` R2 prefix currently contains 485 objects totaling 230,581 bytes. This
inventory is a point-in-time deployment observation, not an SLA or a claim about
future archive size. The compact machine-readable record is
[`benchmarks/results/live-genesis-demo.json`](../benchmarks/results/live-genesis-demo.json).

## Exact `cast` checks

Install [Foundry `cast`](https://getfoundry.sh/cast/overview) and `jq`, substitute the
anonymized Worker hostname, and run:

```sh
export FOSSIL_RPC='https://YOUR-WORKER.workers.dev/rpc'
export WETH='0x4200000000000000000000000000000000000006'

cast rpc --rpc-url "$FOSSIL_RPC" eth_chainId
# "0x2105"

cast rpc --rpc-url "$FOSSIL_RPC" eth_blockNumber
# "0x0"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$WETH" 0x0
# "0x0"

cast rpc --rpc-url "$FOSSIL_RPC" eth_getTransactionCount "$WETH" 0x0
# "0x0"

CODE=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getCode "$WETH" 0x0 | jq -r .)
printf '%s\n' "$(( (${#CODE} - 2) / 2 ))"
# 2041
```

These are standard Ethereum JSON-RPC methods. The final check strips the JSON quotes
and converts the returned `0x`-prefixed bytecode hex length to bytes. The WETH
predeploy has zero native ETH balance and nonce at genesis; its 2,041-byte code confirms
that a zero balance is not being confused with an absent account.

## Why the current range ends at block 0

Fossil publication is forward-only. Extending an exhaustive genesis anchor requires
contiguous, authoritative post-state deltas for every following block, either supplied
directly or derived by replay. The current data source lacks changesets before block
50,000,000, so it cannot bridge block 0 to the available later history. Consequently,
no later complete Base state is claimed yet: the current complete usable state range is
exactly **block `0..0`**.
