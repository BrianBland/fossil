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
publish = importlib.import_module("publish")
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

    def test_remote_budget_prevents_overshoot(self):
        publish.enforce_remote_cap(publish.MAX_BYTES - 1, 1)
        with self.assertRaisesRegex(RuntimeError, "predicted R2 prefix exceeds 5 GB"):
            publish.enforce_remote_cap(publish.MAX_BYTES - 1, 2)

    def test_invalid_replay_never_installs_package(self):
        class Process:
            stdout = io.StringIO("not a replay block\n")
            def poll(self):
                return 0
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "one.jsonl"
            with patch.object(export, "rpc", return_value={"number": "0x0", "hash": HASH}), \
                 patch.object(export.subprocess, "Popen", return_value=Process()):
                with self.assertRaises(ValueError):
                    export.convert(1, 1, self.db, package, "http://localhost", Path("probe"), Path("data"))
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

    def test_main_publishes_and_advances_journal(self):
        published = []

        class FakePublisher:
            def __init__(self, *_args):
                pass

            def publish(self, package):
                if not package.exists():
                    raise AssertionError("staged package is missing")
                published.append(package.name)

        def fake_rpc(_endpoint, method, _params):
            return {"eth_chainId": "0x2105", "eth_syncing": False,
                    "eth_getBlockByNumber": {"number": "0x1"}}[method]

        def fake_convert(_first, _last, _db, package, *_args):
            package.write_text("{}")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            head = root / "mirror" / export.HEAD
            head.parent.mkdir(parents=True)
            head.write_bytes(b"mock")
            arguments = ["export.py", "--first", "1", "--last", "1", "--baseline", "0",
                         "--workspace", tmp, "--datadir", tmp, "--replay", tmp,
                         "--fossil", tmp, "--rpc", "http://localhost", "--bucket", "fossil"]
            with patch.object(sys, "argv", arguments), patch.object(export, "rpc", fake_rpc), \
                 patch.object(export, "parse_head", return_value=(1, 0, b"")), \
                 patch.object(export, "Publisher", FakePublisher), \
                 patch.object(export, "convert", fake_convert):
                export.main()
            self.assertEqual(published, ["1-1.jsonl"])
            with sqlite3.connect(root / "lifetimes.sqlite") as db:
                self.assertEqual(db.execute("SELECT value FROM meta WHERE key='last_block'").fetchone(), (1,))


if __name__ == "__main__":
    unittest.main()
