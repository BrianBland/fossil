#!/usr/bin/env python3
"""Convert read-only Base replay into bounded fossil-export/1 batches with an incarnation journal."""
import argparse
import concurrent.futures
import json
import os
import re
import resource
import sqlite3
import subprocess
import tempfile
import urllib.request
from pathlib import Path

from publish import HEAD, Publisher, enforce_cap, parse_head

GENESIS = "0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd"
MAX_INPUT = 100_000_000
# ponytail: eight read-only replays cap RocksDB memory/I/O; tune against live-node latency.
REPLAY_WORKERS = 8
REPLAY_CHUNK = 125
ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
HASH = re.compile(r"0x[0-9a-f]{64}\Z")
QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def hex_value(value, pattern):
    require(isinstance(value, str) and pattern.fullmatch(value), f"malformed hex value: {value!r}")
    return value


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate key: {key}")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def rpc(endpoint, method, params):
    data = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(endpoint, data, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        result = decode(response.read())
    require(isinstance(result, dict) and "error" not in result and "result" in result,
            f"Base RPC {method} failed")
    return result["result"]


def line(out, record):
    out.write(json.dumps(record, separators=(",", ":")) + "\n")


def lifetime(db, address, previous_exists, exists, wiped):
    require(all(type(v) is bool for v in (previous_exists, exists, wiped)), "invalid lifetime flags")
    row = db.execute("SELECT incarnation, exists_now FROM lifetime WHERE address=?", (address,)).fetchone()
    if row is None:
        # At the system-only baseline, existing accounts have incarnation zero.
        incarnation = 0
    else:
        incarnation, before = row
        require(bool(before) == previous_exists, f"inconsistent account lifetime for {address}")
        if exists and not before:
            incarnation += 1
    if exists and previous_exists and wiped:
        incarnation += 1
    if row is not None or previous_exists or exists:
        db.execute("INSERT OR REPLACE INTO lifetime VALUES (?,?,?)", (address, incarnation, int(exists)))
    return incarnation


def validate_block(block, number, parent):
    require(type(block) is dict and type(block.get("block")) is int and block["block"] == number,
            "replayed blocks are not contiguous")
    require(hex_value(block["parent_hash"], HASH) == parent, "replayed parent hash differs")
    for field in ("hash", "state_root"):
        hex_value(block[field], HASH)
    hex_value(block["timestamp"], QUANTITY)
    for field in ("accounts", "storage", "codes"):
        require(type(block[field]) is dict, f"invalid {field} map")
    for address, entry in block["accounts"].items():
        hex_value(address, ADDRESS)
        require(type(entry) is dict and type(entry.get("state")) is dict, "invalid account entry")
        state = entry["state"]
        require(type(state.get("exists")) is bool and type(entry.get("previous_exists")) is bool
                and type(entry.get("wiped_storage")) is bool, "invalid account flags")
        if state["exists"]:
            for field in ("nonce", "balance"):
                hex_value(state[field], QUANTITY)
            hex_value(state["code_hash"], HASH)
    for address, slots in block["storage"].items():
        hex_value(address, ADDRESS)
        require(type(slots) is dict, "invalid storage map")
        for slot, value in slots.items():
            hex_value(slot, HASH)
            hex_value(value, HASH)
    for digest, code in block["codes"].items():
        hex_value(digest, HASH)
        require(type(code) is str and code.startswith("0x") and len(code) % 2 == 0
                and all(c in "0123456789abcdef" for c in code[2:]), "invalid bytecode")


def replay_chunk(first, last, replay, datadir, directory):
    output = directory / f"{first}-{last}.jsonl"
    process = subprocess.Popen([str(replay), str(datadir), str(first), str(last)],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        with output.open("w") as file:
            count = 0
            while raw := process.stdout.readline(MAX_INPUT + 1):
                require(raw.endswith("\n") and len(raw) <= MAX_INPUT,
                        "replay record exceeds 100 MB")
                block = decode(raw)
                require(type(block) is dict and block.get("block") == first + count,
                        "replay chunk skipped a block")
                file.write(raw)
                count += 1
                require(file.tell() <= MAX_INPUT, "replay chunk exceeds 100 MB")
        require(process.wait() == 0 and count == last - first + 1, "replay chunk failed")
        return output
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)


def parallel_replay(first, last, replay, datadir, directory):
    with tempfile.TemporaryDirectory(prefix=".fossil-replay-", dir=directory) as temp:
        temp = Path(temp)
        with concurrent.futures.ThreadPoolExecutor(max_workers=REPLAY_WORKERS) as pool:
            chunks = [pool.submit(replay_chunk, start, min(start + REPLAY_CHUNK - 1, last),
                                  replay, datadir, temp)
                      for start in range(first, last + 1, REPLAY_CHUNK)]
            for job in chunks:
                with job.result().open() as file:
                    yield from file


def convert(first, last, db, destination, endpoint, replay, datadir):
    previous = rpc(endpoint, "eth_getBlockByNumber", [hex(first - 1), False])
    require(type(previous) is dict and int(previous["number"], 16) == first - 1,
            "missing preceding canonical Base block")
    parent = hex_value(previous["hash"], HASH)
    seen_code = set()
    fd, temporary = tempfile.mkstemp(prefix=".native-export-", dir=destination.parent)
    lines = None
    try:
        with os.fdopen(fd, "w") as out:
            line(out, {"type": "header", "schema": "fossil-export/1", "chain_id": "0x2105",
                       "genesis_hash": GENESIS, "mode": "delta", "preceding_number": hex(first - 1),
                       "preceding_hash": parent})
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            require(hard >= 65_536, "read-only Reth requires a nofile hard limit of at least 65536")
            if soft < 65_536:
                resource.setrlimit(resource.RLIMIT_NOFILE, (65_536, hard))
            lines = parallel_replay(first, last, replay, datadir, destination.parent)
            count = 0
            for raw in lines:
                block = decode(raw)
                number = first + count
                validate_block(block, number, parent)
                parent = block["hash"]
                line(out, {"type": "block", "number": hex(number), "hash": parent,
                           "parent_hash": block["parent_hash"], "state_root": block["state_root"],
                           "timestamp": block["timestamp"]})
                events = 0
                for address, entry in sorted(block["accounts"].items()):
                    state = entry["state"]
                    exists = state["exists"]
                    incarnation = lifetime(db, address, entry["previous_exists"], exists,
                                           entry["wiped_storage"])
                    record = {"type": "account", "block": hex(number), "address": address,
                              "exists": exists, "incarnation": hex(incarnation)}
                    if exists:
                        record.update(nonce=state["nonce"], balance=state["balance"],
                                      code_hash=state["code_hash"])
                    line(out, record)
                    events += 1
                for address, slots in sorted(block["storage"].items()):
                    row = db.execute("SELECT incarnation, exists_now FROM lifetime WHERE address=?", (address,)).fetchone()
                    require(row is None or bool(row[1]), f"storage update for absent account {address}")
                    incarnation = row[0] if row else 0
                    for slot, value in sorted(slots.items()):
                        line(out, {"type": "storage", "block": hex(number), "address": address,
                                   "incarnation": hex(incarnation), "slot": slot, "value": value})
                        events += 1
                for digest, code in sorted(block["codes"].items()):
                    if digest not in seen_code:
                        line(out, {"type": "code", "code_hash": digest, "bytes": code})
                        seen_code.add(digest)
                        events += 1
                line(out, {"type": "block_end", "number": hex(number), "event_count": hex(events)})
                count += 1
                require(out.tell() <= MAX_INPUT, "bounded normalized input exceeds 100 MB")
            require(count == last - first + 1, "replay skipped blocks")
            line(out, {"type": "trailer", "end_number": hex(last), "end_hash": parent,
                       "block_count": hex(count)})
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, destination)
    finally:
        if lines is not None:
            lines.close()
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--first", required=True, type=int)
    p.add_argument("--last", required=True, type=int)
    p.add_argument("--baseline", required=True, type=int, help="previous published system-only block; all lifetimes zero")
    p.add_argument("--workspace", required=True, type=Path)
    p.add_argument("--datadir", required=True, type=Path)
    p.add_argument("--replay", required=True, type=Path)
    p.add_argument("--fossil", required=True, type=Path)
    p.add_argument("--rpc", required=True, help="local Base JSON-RPC URL")
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="v1/")
    p.add_argument("--dry-run", action="store_true", help="convert one epoch, rollback journal; no R2 writes")
    args = p.parse_args()
    if args.first <= 0 or args.baseline < 0 or args.last < args.first:
        p.error("expected positive contiguous block range")
    if args.dry_run and args.last - args.first >= 1000:
        p.error("dry-run accepts at most one 1000-block epoch")
    require(rpc(args.rpc, "eth_chainId", []) == "0x2105"
            and rpc(args.rpc, "eth_syncing", []) is False, "wrong or syncing Base node")
    finalized = rpc(args.rpc, "eth_getBlockByNumber", ["finalized", False])
    require(type(finalized) is dict and args.last <= int(finalized["number"], 16),
            "unfinalized block requested")
    args.workspace.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(args.workspace / "lifetimes.sqlite")
    try:
        db.execute("CREATE TABLE IF NOT EXISTS lifetime (address TEXT PRIMARY KEY, incarnation INTEGER NOT NULL, exists_now INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        head = db.execute("SELECT value FROM meta WHERE key='last_block'").fetchone()
        require((head is None and args.first == args.baseline + 1)
                or (head is not None and head[0] == args.first - 1),
                "incarnation journal does not precede batch")
        publisher = None if args.dry_run else Publisher(args.workspace, args.fossil, args.bucket, args.prefix)
        for first in range(args.first, args.last + 1, 1000):
            last = min(first + 999, args.last)
            require(parse_head((args.workspace / "mirror" / HEAD).read_bytes())[1] == first - 1,
                    "local publication head does not precede batch")
            enforce_cap(args.workspace)
            db.execute("BEGIN IMMEDIATE")
            package = args.workspace / f"{first}-{last}.jsonl"
            try:
                convert(first, last, db, package, args.rpc, args.replay, args.datadir)
                enforce_cap(args.workspace)
                print(f"PREPARED {first}..{last} bytes={package.stat().st_size}", flush=True)
                if args.dry_run:
                    db.rollback()
                    return
                publisher.publish(package)
                db.execute("INSERT OR REPLACE INTO meta VALUES ('last_block', ?)", (last,))
                db.commit()
                package.unlink()
                print(f"EXPORT_COMPLETE through={last}", flush=True)
            except Exception:
                db.rollback()
                raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
