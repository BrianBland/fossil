"""Run with python3 -m unittest discover -s tools/base-export -p 'test_*.py'."""
import importlib
import io
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

export = importlib.import_module("export")
ADDRESS = "0x" + "12" * 20
HASH = "0x" + "34" * 32


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE lifetime (address TEXT PRIMARY KEY, incarnation INTEGER NOT NULL, exists_now INTEGER NOT NULL)")

    def tearDown(self):
        self.db.close()

    def test_destroy_recreate_and_same_block_wipe(self):
        # Existing baseline account, destroyed, then recreated; reincarnation isolates slots.
        self.assertEqual(export.lifetime(self.db, ADDRESS, True, False, True), 0)
        self.assertEqual(export.lifetime(self.db, ADDRESS, False, True, False), 1)
        # A destroy + recreate within one block ends live, but wipe_storage must advance again.
        self.assertEqual(export.lifetime(self.db, ADDRESS, True, True, True), 2)
        self.assertEqual(export.lifetime(self.db, ADDRESS, True, True, False), 2)
        self.assertEqual(self.db.execute("SELECT incarnation, exists_now FROM lifetime").fetchone(), (2, 1))

    def test_malformed_input_fails_closed(self):
        with self.assertRaises(ValueError):
            export.lifetime(self.db, ADDRESS, False, "false", False)
        with self.assertRaises(ValueError):
            export.decode('{"block":1,"block":2}')
        block = {"block": 12, "hash": HASH, "parent_hash": HASH, "state_root": HASH,
                 "timestamp": "0x1", "accounts": {ADDRESS: {"state": {"exists": True,
                 "nonce": "0x0", "balance": "0x0", "code_hash": HASH},
                 "previous_exists": False, "wiped_storage": False}}, "storage": {}, "codes": {}}
        export.validate_block(block, 12, HASH)
        block["accounts"][ADDRESS]["wiped_storage"] = "false"
        with self.assertRaises(ValueError):
            export.validate_block(block, 12, HASH)
        block["accounts"][ADDRESS]["wiped_storage"] = False
        block["storage"] = {ADDRESS: {"0x1": HASH}}
        with self.assertRaises(ValueError):
            export.validate_block(block, 12, HASH)
        with self.assertRaises(ValueError):
            export.decode("not JSON")

    def test_local_cap_stops_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "big").write_bytes(b"x" * 10)
            export.enforce_cap(Path(tmp), 10)
            with self.assertRaisesRegex(export.CapReached, "exceeds the cap"):
                export.enforce_cap(Path(tmp), 9)

    def test_invalid_replay_never_installs_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "one.jsonl"
            with self.assertRaises(ValueError):
                export.convert(1, 1, self.db, package, HASH, iter(["not a replay block\n"]))
            self.assertFalse(package.exists())
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_parallel_replay_is_bounded_and_keeps_block_order(self):
        in_flight = 0
        peak = 0
        lock = threading.Lock()

        def replay_chunk(first, last, _replay, _datadir, directory):
            nonlocal in_flight, peak
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            time.sleep(0.02)
            path = directory / f"{first}.jsonl"
            path.write_text("".join(json.dumps({"block": n}) + "\n" for n in range(first, last + 1)))
            with lock:
                in_flight -= 1
            return path

        with tempfile.TemporaryDirectory() as tmp, patch.object(export, "replay_chunk", replay_chunk):
            blocks = [json.loads(line)["block"] for line in export.parallel_replay(
                1, 1000, Path("probe"), Path("data"), Path(tmp))]
        self.assertEqual(blocks, list(range(1, 1001)))
        self.assertTrue(2 <= peak <= export.REPLAY_WORKERS)

    def run_main(self, root, first, last, head, published, fail=False):
        def fake_publish(_fossil, store, package, last, end_hash):
            if fail:
                raise RuntimeError("R2 down")
            if not package.exists():
                raise AssertionError("spooled package is missing")
            published.append((package.name, last, end_hash))

        def fake_rpc(_endpoint, method, _params):
            return {"eth_chainId": "0x2105", "eth_syncing": False,
                    "eth_getBlockByNumber": {"number": "0x100000"}}[method]

        def fake_convert(first, last, _db, package, _parent, _lines):
            package.write_text('{}\n{"type":"trailer","end_hash":"%s"}\n' % HASH)
            return HASH

        arguments = ["export.py", "--first", str(first), "--last", str(last),
                     "--workspace", str(root), "--datadir", str(root), "--replay", str(root),
                     "--fossil", str(root), "--rpc", "http://localhost", "--store", "file:///x"]
        with patch.object(sys, "argv", arguments), patch.object(export, "rpc", fake_rpc), \
             patch.object(export, "publish", fake_publish), \
             patch.object(export, "head_number", return_value=head), \
             patch.object(export, "preceding_hash", return_value=HASH), \
             patch.object(export, "parallel_replay", lambda *a: (x for x in ())), \
             patch.object(export, "convert", fake_convert):
            export.main()

    def test_main_publishes_in_order_and_advances_journal(self):
        published = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".fossil-replay-stale").mkdir()
            (root / ".native-export-stale").write_text("partial")
            self.run_main(root, 1, 2500, 0, published)
            self.assertEqual([name for name, _, _ in published],
                             ["1-1000.jsonl", "1001-2000.jsonl", "2001-2500.jsonl"])
            self.assertEqual(sorted(p.name for p in root.iterdir()), ["lifetimes.sqlite"])
            with sqlite3.connect(root / "lifetimes.sqlite") as db:
                self.assertEqual(db.execute("SELECT value FROM meta WHERE key='last_block'").fetchone(), (2500,))

    def test_restart_publishes_spooled_packages_first(self):
        published = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with sqlite3.connect(root / "lifetimes.sqlite") as db:
                db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
                db.execute("INSERT INTO meta VALUES ('last_block', 3000)")
            trailer = '{}\n{"type":"trailer","end_hash":"%s"}\n' % HASH
            (root / "1-1000.jsonl").write_text(trailer)       # published, not yet removed
            (root / "1001-2000.jsonl").write_text(trailer)    # converted, not published
            (root / "2001-3000.jsonl").write_text(trailer)
            (root / "3001-4000.jsonl").write_text(trailer)    # converted, journal rolled back
            self.run_main(root, 3001, 3500, 1000, published)
            self.assertEqual([name for name, _, _ in published],
                             ["1001-2000.jsonl", "2001-3000.jsonl", "3001-3500.jsonl"])
            # A head behind the journal with no spooled packages cannot resume.
            with self.assertRaisesRegex(ValueError, "spool does not cover"):
                self.run_main(root, 3501, 4000, 2000, [])

    def test_background_publication_failure_stops_the_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "background publication failed"):
                self.run_main(root, 1, 9000, 0, [], fail=True)
            # The journal ran ahead only as far as the durable spool allows.
            spooled = sorted(p.name for p in root.glob("*-*.jsonl"))
            self.assertLessEqual(len(spooled), export.SPOOL_DEPTH + 1)

    def test_publish_waits_out_compaction_backlog(self):
        class Result:
            def __init__(self, code, stderr):
                self.returncode, self.stderr = code, stderr
        results = [Result(1, "Error: tiered L0 compaction backlog is full"), Result(0, "")]
        with patch.object(export.subprocess, "run", side_effect=lambda *a, **k: results.pop(0)), \
             patch.object(export.time, "sleep"):
            export.publish(Path("fossil"), "file:///x", Path("p.jsonl"), 1, HASH)
        self.assertEqual(results, [])
        with patch.object(export.subprocess, "run", return_value=Result(1, "boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                export.publish(Path("fossil"), "file:///x", Path("p.jsonl"), 1, HASH)


if __name__ == "__main__":
    unittest.main()
