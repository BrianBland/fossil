# Live Base R2/Cloudflare Worker week demo

A public, read-only Rust/WASM Cloudflare Worker serves a sparse Fossil format-v1
archive directly from R2:

- JSON-RPC endpoint: <https://fossil-r2-gateway.brian-t-bland.workers.dev/rpc>
- R2 bucket: `base-mainnet-fossil`
- query archive prefix: `fossil-demo/`
- chain ID: `8453` (`0x2105`)
- inclusive block range: `51,232,601..51,535,000`
  (`0x30dbf59..0x3125c98`)
- timestamp range: `2026-09-12T23:09:09Z..2026-09-19T23:09:07Z`
- coverage: 302,400 blocks in 303 epochs

The endpoint accepts only the [standard methods implemented by the
Worker](../worker/README.md#api-and-limits). It has no write, delete, or list API and
requires no credentials. Do not put R2 credentials in commands or URLs. Fossil values
are post-execution state for the selected block.

## Exact sparse overlay

This is an exact query overlay for a deliberately small set of keys, not an
arbitrary-address-complete Base archive. It records every-block post-state changes in
the supported range for:

- the native ETH balance of the Sequencer Fee Vault at
  `0x4200000000000000000000000000000000000011`;
- the native ETH balance held by the WETH contract at
  `0x4200000000000000000000000000000000000006` (this is **not** an ERC-20 WETH
  balance query); and
- Aerodrome volatile WETH/USDC pool
  `0xcdac0d6c6c59727a65f871236188350531885c43`, storage slots `20`, `21`, and
  `22` for reserve0, reserve1, and the reserve timestamp.

For this pool, token0 is WETH with 18 decimals and token1 is native USDC with 6
decimals. The spot reserve ratio in USDC/WETH is:

```text
int(reserve1) * 1e12 / int(reserve0)
```

Queries for other addresses or slots can return sparse-overlay defaults and must not
be interpreted as complete Base state. The supported methods are broader than the
keys populated by this demo.

## Copy-paste verification

The following commands require [Foundry `cast`](https://getfoundry.sh/cast/overview),
`jq`, and Python 3. They use standard Ethereum JSON-RPC method names rather than
Fossil-specific methods.

```sh
export FOSSIL_RPC='https://fossil-r2-gateway.brian-t-bland.workers.dev/rpc'
export VAULT='0x4200000000000000000000000000000000000011'
export WETH='0x4200000000000000000000000000000000000006'
export POOL='0xcdac0d6c6c59727a65f871236188350531885c43'
export START_BLOCK='0x30dbf59'
export MID_BLOCK='0x3100df8'
export END_BLOCK='0x3125c98'
export BLOCK="$START_BLOCK"

cast rpc --rpc-url "$FOSSIL_RPC" web3_clientVersion
cast rpc --rpc-url "$FOSSIL_RPC" eth_chainId
# "0x2105"
cast rpc --rpc-url "$FOSSIL_RPC" eth_blockNumber
# "0x3125c98"

# Sequencer Fee Vault native ETH balance.
cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$VAULT" "$BLOCK"
# "0x1b080ed91d2d651ac9"

# Native ETH held by the WETH contract; this is not ERC-20 balanceOf.
cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$WETH" "$BLOCK"
# "0x32b2a8b601b5084031ab"

# Aerodrome reserve0 (WETH), reserve1 (native USDC), and reserve timestamp.
cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x14 "$BLOCK"
# "0x00000000000000000000000000000000000000000000005af2d09c413d92fbce"
cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x15 "$BLOCK"
# "0x000000000000000000000000000000000000000000000000000003d917036c1f"
cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x16 "$BLOCK"
```

This loop queries every supported demo state type at the start, midpoint, and end
sample blocks, then calculates the decimal-adjusted pool price with Python:

```sh
for BLOCK in "$START_BLOCK" "$MID_BLOCK" "$END_BLOCK"; do
  vault=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$VAULT" "$BLOCK" | jq -r .)
  weth_eth=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getBalance "$WETH" "$BLOCK" | jq -r .)
  reserve0=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x14 "$BLOCK" | jq -r .)
  reserve1=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x15 "$BLOCK" | jq -r .)
  reserve_timestamp=$(cast rpc --rpc-url "$FOSSIL_RPC" eth_getStorageAt "$POOL" 0x16 "$BLOCK" | jq -r .)
  price=$(python3 - "$reserve0" "$reserve1" <<'PY'
import sys
reserve0, reserve1 = (int(value, 16) for value in sys.argv[1:])
print(reserve1 * 10**12 / reserve0)
PY
)
  printf '%s vault=%s weth_contract_eth=%s reserve0=%s reserve1=%s reserve_timestamp=%s price_usdc_per_weth=%s\n' \
    "$BLOCK" "$vault" "$weth_eth" "$reserve0" "$reserve1" "$reserve_timestamp" "$price"
done
```

`jq` is used only to remove JSON string quotes before passing values to Python.

## Published sample results

| Sample | Block | Vault ETH | WETH contract native ETH | reserve0 | reserve1 | USDC/WETH |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| start | `0x30dbf59` (51,232,601) | 498.642730111474146 | 239414.001494723733238187 | `0x5af2d09c413d92fbce` | `0x3d917036c1f` | 2521.857155719857 |
| midpoint | `0x3100df8` (51,383,800) | 129.010182857849921494 | 259649.979208333153025763 | `0x44e146e5e6b19e47d3` | `0x2c5fc0026fd` | 2399.9150434278786 |
| end | `0x3125c98` (51,535,000) | 249.166524872288029600 | 238894.129307330587280079 | `0x5cc365628c29dd045d` | `0x416b05ff1dd` | 2627.131151463924 |

The compact machine-readable result is
[`benchmarks/results/live-week-demo.json`](../benchmarks/results/live-week-demo.json).

## Build, publication, and bucket size

The query archive built locally in 294.920 seconds and occupied 81,245,782 bytes in
2,793 files. Uploading 2,791 immutable objects plus the mutable head to R2 took 71
seconds.

A separate real-week physical-layout corpus built in 122.865 seconds: 607 files and
4,427,761,942 bytes. Its R2 upload took 53 seconds. It is retained under
`fossil-week-layout` for physical-layout evidence and is **not** directly queryable as
forward state. The two builds and two uploads totaled 541.785 seconds (about 9m02s).

After both uploads, the bucket held 4,035 objects and 4,514,343,105 bytes (4.514 GB,
4.204 GiB), below the 5 GB bucket cap. These are one local build/publication run's
observations, not sustained throughput or an SLA.

## Round-trip latency observation

Seven sequential requests of each key were issued from a Mac in Seattle. The table
reports median, first-request, and maximum-of-7 end-to-end round-trip milliseconds,
rounded to the nearest millisecond. These are environment-specific observations, not
a service SLA. “Maximum of 7” is the observed maximum, not a statistically
interpolated p95. The raw samples are retained in the machine-readable result.

| Sample block | Statistic | Vault balance | WETH contract ETH | reserve0 | reserve1 |
| --- | --- | ---: | ---: | ---: | ---: |
| start | median | 546 ms | 744 ms | 689 ms | 662 ms |
| start | first request | 517 ms | 755 ms | 689 ms | 648 ms |
| start | maximum of 7 | 766 ms | 856 ms | 716 ms | 782 ms |
| midpoint | median | 570 ms | 603 ms | 2747 ms | 2618 ms |
| midpoint | first request | 654 ms | 768 ms | 2747 ms | 2461 ms |
| midpoint | maximum of 7 | 694 ms | 768 ms | 3209 ms | 2734 ms |
| end | median | 450 ms | 423 ms | 3585 ms | 4349 ms |
| end | first request | 594 ms | 644 ms | 3688 ms | 3073 ms |
| end | maximum of 7 | 594 ms | 644 ms | 4114 ms | 5742 ms |
