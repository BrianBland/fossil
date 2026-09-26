#!/usr/bin/env python3
"""Replay Base from genesis into bounded fossil-export/1 packages and append them to a tiered v1 archive."""
import argparse
import collections
import concurrent.futures
import itertools
import json
import os
import re
import resource
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

GENESIS = "0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd"
MAX_INPUT = 100_000_000
# ponytail: eight read-only replays cap RocksDB memory/I/O; tune against live-node latency.
REPLAY_WORKERS = 8
REPLAY_CHUNK = 125
ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
HASH = re.compile(r"0x[0-9a-f]{64}\Z")
QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")
MAX_BYTES = 5_000_000_000  # decimal GB, for both the local workspace and the remote prefix
REMOTE_CHECK_EPOCHS = 25
BACKLOG_RETRY_SECONDS = 10
# Converted packages allowed to wait for publication.
SPOOL_DEPTH = 2


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
    """Replay blocks in order with at most REPLAY_WORKERS chunks running and a
    bounded number finished ahead, across package boundaries."""
    with tempfile.TemporaryDirectory(prefix=".fossil-replay-", dir=directory) as temp:
        temp = Path(temp)
        with concurrent.futures.ThreadPoolExecutor(max_workers=REPLAY_WORKERS) as pool:
            starts = iter(range(first, last + 1, REPLAY_CHUNK))
            pending = collections.deque()
            def submit():
                start = next(starts, None)
                if start is not None:
                    pending.append(pool.submit(replay_chunk, start,
                                               min(start + REPLAY_CHUNK - 1, last),
                                               replay, datadir, temp))
            for _ in range(REPLAY_WORKERS * 2):
                submit()
            while pending:
                path = pending.popleft().result()
                submit()
                with path.open() as file:
                    yield from file
                path.unlink()


def preceding_hash(endpoint, first):
    previous = rpc(endpoint, "eth_getBlockByNumber", [hex(first - 1), False])
    require(type(previous) is dict and int(previous["number"], 16) == first - 1,
            "missing preceding canonical Base block")
    return hex_value(previous["hash"], HASH)


def convert(first, last, db, destination, parent, lines):
    """Write blocks first..last from the ordered replay stream `lines`; returns the end hash."""
    seen_code = set()
    fd, temporary = tempfile.mkstemp(prefix=".native-export-", dir=destination.parent)
    try:
        with os.fdopen(fd, "w") as out:
            line(out, {"type": "header", "schema": "fossil-export/1", "chain_id": "0x2105",
                       "genesis_hash": GENESIS, "mode": "delta", "preceding_number": hex(first - 1),
                       "preceding_hash": parent})
            count = 0
            for raw in itertools.islice(lines, last - first + 1):
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
        return parent
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def workspace_size(root):
    """Bytes under root; files removed concurrently (published packages, consumed
    replay chunks) are skipped."""
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass
    return total


class CapReached(Exception):
    """A byte cap was reached; the export stops cleanly (exit 0) to be resumed later."""


def enforce_cap(root, cap):
    size = workspace_size(root)
    if size > cap:
        raise CapReached(f"STOPPED: {size} local workspace bytes exceeds the cap")
    return size


def remote_bytes(store):
    """Total bytes under an s3:// store prefix (superseded compaction output included)."""
    import boto3
    url = urllib.parse.urlparse(store)
    prefix = url.path.strip("/") + "/"
    s3 = boto3.client("s3", endpoint_url=os.environ["CF_S3_API_ENDPOINT"], region_name="auto",
                      aws_access_key_id=os.environ["CF_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["CF_SECRET_ACCESS_KEY"])
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=url.netloc, Prefix=prefix):
        total += sum(obj["Size"] for obj in page.get("Contents", []))
    return total


def fossil_env():
    env = dict(os.environ)
    if "CF_S3_API_ENDPOINT" in env:
        env.setdefault("FOSSIL_S3_ENDPOINT", env["CF_S3_API_ENDPOINT"])
        env.setdefault("AWS_REGION", "auto")
        env.setdefault("AWS_ACCESS_KEY_ID", env.get("CF_ACCESS_KEY_ID", ""))
        env.setdefault("AWS_SECRET_ACCESS_KEY", env.get("CF_SECRET_ACCESS_KEY", ""))
    return env


def publish(fossil, store, package, last, end_hash):
    """Append one package; wait out compaction backlog; idempotent on retry."""
    command = [str(fossil), "archive", "--input", str(package), "--store", store,
               "--chain-id", "0x2105", "--gate", "finalized",
               "--finalized-head", f"{last}:{end_hash}"]
    while True:
        result = subprocess.run(command, capture_output=True, text=True, timeout=1800,
                                env=fossil_env())
        if result.returncode == 0:
            return
        if "backlog" in result.stderr:
            print("WAITING for compaction backlog", flush=True)
            time.sleep(BACKLOG_RETRY_SECONDS)
            continue
        raise RuntimeError(f"fossil archive failed: {result.stderr.strip()[-2000:]}")


def head_number(fossil, store):
    """Published tiered head block number."""
    result = subprocess.run([str(fossil), "verify", "--store", store, "--chain-id", "0x2105"],
                            capture_output=True, text=True, timeout=600, env=fossil_env())
    require(result.returncode == 0, f"fossil verify failed: {result.stderr.strip()[-500:]}")
    match = re.search(r"through (\d+) ", result.stdout)
    require(match is not None, "unexpected fossil verify output")
    return int(match.group(1))


class Publisher:
    """Publishes spooled packages in order on a background thread, so replay and
    conversion of later packages overlap run building, uploads and the head CAS.
    At most SPOOL_DEPTH converted packages wait on disk."""

    def __init__(self, fossil, store, spooled):
        self.fossil, self.store = fossil, store
        self.slots = threading.Semaphore(SPOOL_DEPTH)
        self.queue = collections.deque()
        self.ready = threading.Condition()
        self.error = None
        self.closed = False
        for package in spooled:
            self.slots.acquire()
            self.queue.append(package)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def check(self):
        if self.error is not None:
            raise RuntimeError("background publication failed") from self.error

    def reserve(self):
        while not self.slots.acquire(timeout=1):
            self.check()
        self.check()

    def submit(self, package):
        with self.ready:
            self.queue.append(package)
            self.ready.notify()

    def finish(self):
        with self.ready:
            self.closed = True
            self.ready.notify()
        while self.thread.is_alive():
            self.thread.join(timeout=1)
        self.check()

    def close(self):
        with self.ready:
            self.closed = True
            self.ready.notify()

    def run(self):
        try:
            while True:
                with self.ready:
                    while not self.queue and not self.closed:
                        self.ready.wait()
                    if not self.queue:
                        return
                    package = self.queue[0]
                last = int(package.stem.split("-")[1])
                end_hash = json.loads(package.read_text().splitlines()[-1])["end_hash"]
                publish(self.fossil, self.store, package, last, end_hash)
                package.unlink()
                with self.ready:
                    self.queue.popleft()
                self.slots.release()
                print(f"EXPORT_COMPLETE through={last}", flush=True)
        except BaseException as error:  # surfaced to the converter via check()
            self.error = error


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--first", required=True, type=int)
    p.add_argument("--last", required=True, type=int)
    p.add_argument("--workspace", required=True, type=Path)
    p.add_argument("--datadir", required=True, type=Path)
    p.add_argument("--replay", required=True, type=Path)
    p.add_argument("--fossil", required=True, type=Path)
    p.add_argument("--rpc", required=True, help="local Base JSON-RPC URL")
    p.add_argument("--store", required=True, help="tiered store URI, e.g. s3://bucket/prefix")
    p.add_argument("--cap-bytes", type=int, default=MAX_BYTES)
    p.add_argument("--dry-run", action="store_true", help="convert one epoch, rollback journal; no writes")
    args = p.parse_args()
    if args.first <= 0 or args.last < args.first:
        p.error("expected positive contiguous block range")
    if args.dry_run and args.last - args.first >= 1000:
        p.error("dry-run accepts at most one 1000-block epoch")
    require(rpc(args.rpc, "eth_chainId", []) == "0x2105"
            and rpc(args.rpc, "eth_syncing", []) is False, "wrong or syncing Base node")
    finalized = rpc(args.rpc, "eth_getBlockByNumber", ["finalized", False])
    require(type(finalized) is dict and args.last <= int(finalized["number"], 16),
            "unfinalized block requested")
    args.workspace.mkdir(parents=True, exist_ok=True)
    # One exporter per workspace: staging left by a killed run is never resumed.
    for stale in [*args.workspace.glob(".fossil-replay-*"), *args.workspace.glob(".native-export-*")]:
        shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
    db = sqlite3.connect(args.workspace / "lifetimes.sqlite")
    try:
        db.execute("CREATE TABLE IF NOT EXISTS lifetime (address TEXT PRIMARY KEY, incarnation INTEGER NOT NULL, exists_now INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        head = db.execute("SELECT value FROM meta WHERE key='last_block'").fetchone()
        # Genesis accounts start at incarnation zero; the block-zero anchor is published first.
        require((head is None and args.first == 1) or (head is not None and head[0] == args.first - 1),
                "incarnation journal does not precede batch")
        journal = head[0] if head else 0
        spooled = sorted(args.workspace.glob("*-*.jsonl"), key=lambda p: int(p.name.split("-")[0]))
        if not args.dry_run:
            # Journal commits run ahead of publication; the spool holds the gap.
            published = head_number(args.fossil, args.store)
            covered = published
            for package in spooled:
                start, end = (int(x) for x in package.stem.split("-"))
                if end <= published or end > journal:
                    # Published but not yet removed, or converted but never
                    # committed to the journal (its lifetimes were rolled back).
                    package.unlink()
                    continue
                require(start == covered + 1, f"spool gap before {package.name}")
                covered = end
            require(covered == journal, "spool does not cover the journal beyond the head")
            spooled = [p for p in spooled if p.exists()]
        publisher = None if args.dry_run else Publisher(args.fossil, args.store, spooled)
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        require(hard >= 65_536, "read-only Reth requires a nofile hard limit of at least 65536")
        if soft < 65_536:
            resource.setrlimit(resource.RLIMIT_NOFILE, (65_536, hard))
        lines = parallel_replay(args.first, args.last, args.replay, args.datadir, args.workspace)
        parent = preceding_hash(args.rpc, args.first)
        remote_checked = 0
        try:
            for first in range(args.first, args.last + 1, 1000):
                last = min(first + 999, args.last)
                enforce_cap(args.workspace, args.cap_bytes)
                if not args.dry_run and args.store.startswith("s3://") and remote_checked % REMOTE_CHECK_EPOCHS == 0:
                    size = remote_bytes(args.store)
                    if size > args.cap_bytes:
                        raise CapReached(f"STOPPED: remote prefix holds {size} bytes")
                    print(f"REMOTE_BYTES {size}", flush=True)
                remote_checked += 1
                if publisher:
                    publisher.reserve()
                db.execute("BEGIN IMMEDIATE")
                package = args.workspace / f"{first}-{last}.jsonl"
                try:
                    parent = convert(first, last, db, package, parent, lines)
                    enforce_cap(args.workspace, args.cap_bytes)
                    print(f"PREPARED {first}..{last} bytes={package.stat().st_size}", flush=True)
                    if args.dry_run:
                        db.rollback()
                        return
                    db.execute("INSERT OR REPLACE INTO meta VALUES ('last_block', ?)", (last,))
                    db.commit()
                except Exception:
                    db.rollback()
                    package.unlink(missing_ok=True)
                    raise
                publisher.submit(package)
            if publisher:
                publisher.finish()
        finally:
            lines.close()
            if publisher:
                publisher.close()
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except CapReached as stop:
        print(stop, flush=True)
