# State semantics

Fossil stores complete values **after execution of block N**. A canonical block is
identified by schema, chain ID, genesis hash, number, hash, and parent hash. Every
block after the bootstrap boundary must immediately follow a committed parent.

## Bootstrap and folding

An anchor package is exhaustive at block S: it contains every live account, every
nonzero storage slot, and referenced code. Tiered v1 requires S = 0 (genesis). A delta
package identifies the exact preceding published number/hash and records only values
that changed. No record means inherit the prior value; it never means zero.

The internal meanings remain distinct:

- an account record with `exists:false` makes the account absent;
- an existing zero-valued account remains distinguishable from an absent account;
- a 32-byte zero storage record shadows all older values for that incarnation;
- an absent storage record inherits the older value;
- an address recreation uses a new exporter-assigned `incarnation`, preventing slots
  from an older lifetime from being visible.

The RPC layer follows Ethereum read conventions: absent accounts produce zero nonce,
zero balance, empty code, and zero storage. It does not expose account existence.

## Source contract

Input is `fossil-export/1` JSON Lines, optionally wrapped in one zstd stream. Lowercase
fixed-width hex is required for addresses, hashes, slots, and storage values;
Ethereum quantities must be canonical (`0x0`, never `0x00`). Bytecode is even-length
hex data and is verified with Keccak-256. Records and logical keys are strictly
ordered and unique within a block.

The exporter is authoritative for post-state and incarnation assignment. Fossil does
not derive transaction-level deletion behavior, turn unwind values into forward
values, or prove that the supplied flat state reconstructs `state_root`.
