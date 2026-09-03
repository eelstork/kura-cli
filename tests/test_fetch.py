"""kura_cli.fetch, driven against a stub kura store over real HTTP.

The stub speaks the three routes the fetch uses — GET /manifest, POST /blobs2,
GET /blob/{digest} — with a bearer gate, and can be told to refuse a /blobs2
batch so the fallback (split, then one-at-a-time /blob) is exercised for real.
"""

import base64
import contextlib
import hashlib
import io
import json
import os
import struct
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kura_cli

KEY = "test-key"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StubStore:
    """A tiny content-addressed store with named packages."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.packages: dict[str, dict[str, str]] = {}
        self.refuse_blobs2_over = None  # refuse a batch larger than this many digests
        self.blobs2_calls = 0
        self.blob_calls = 0

    def add(self, package: str, path: str, data: bytes) -> str:
        d = _digest(data)
        self.blobs[d] = data
        self.packages.setdefault(package, {})[f"{package}/{path}"] = d
        return d


def _make_handler(store: StubStore):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # keep the test output quiet
            pass

        def _auth_ok(self) -> bool:
            if self.headers.get("Authorization") == f"Bearer {KEY}":
                return True
            self._json(401, {"detail": "Missing bearer token"})
            return False

        def _json(self, code: int, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if not self._auth_ok():
                return
            u = urlparse(self.path)
            if u.path == "/manifest":
                pkg = parse_qs(u.query).get("package", [None])[0]
                self._json(200, store.packages.get(pkg, {}))
            elif u.path.startswith("/blob/"):
                store.blob_calls += 1
                d = u.path[len("/blob/"):]
                if d not in store.blobs:
                    self._json(404, {"detail": "no such blob"})
                    return
                data = store.blobs[d]
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._json(404, {"detail": "Not Found"})

        def do_POST(self):
            if not self._auth_ok():
                return
            if urlparse(self.path).path != "/blobs2":
                self._json(404, {"detail": "Not Found"})
                return
            store.blobs2_calls += 1
            length = int(self.headers.get("Content-Length", 0))
            digests = json.loads(self.rfile.read(length))["digests"]
            wanted = list(dict.fromkeys(digests))
            if store.refuse_blobs2_over is not None and len(wanted) > store.refuse_blobs2_over:
                # stand in for the real 502: a batch too big to answer
                self._json(502, {"detail": "Bad Gateway"})
                return
            missing = [d for d in wanted if d not in store.blobs]
            if missing:
                self._json(404, {"detail": {"missing": missing}})
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            for d in wanted:
                data = store.blobs[d]
                head = d.encode("ascii")
                self.wfile.write(struct.pack(">I", len(head)) + head + struct.pack(">Q", len(data)))
                self.wfile.write(data)

    return Handler


class FetchTest(unittest.TestCase):
    def setUp(self):
        self.store = StubStore()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self.store))
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        host, port = self.httpd.server_address
        self.url = f"http://{host}:{port}"
        self._dir = tempfile.TemporaryDirectory()
        self.dest = Path(self._dir.name)

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self._dir.cleanup()

    def _fetch(self, package="demo", **kw):
        kw.setdefault("url", self.url)
        kw.setdefault("key", KEY)
        return kura_cli.fetch(package, self.dest, **kw)

    def test_materializes_the_package_tree(self):
        self.store.add("demo", "src/a.ts", b"alpha")
        self.store.add("demo", "public/big.bin", bytes(range(256)) * 400)
        res = self._fetch()
        # prefix stripped by default: files land at dest/<relative path>
        self.assertEqual((self.dest / "src/a.ts").read_bytes(), b"alpha")
        self.assertEqual((self.dest / "public/big.bin").read_bytes(), bytes(range(256)) * 400)
        self.assertEqual(res["written"], 2)

    def test_binary_content_round_trips_exactly(self):
        payload = bytes(range(256)) + b'"\\\x00 not json'
        self.store.add("demo", "raw.bin", payload)
        self._fetch()
        self.assertEqual((self.dest / "raw.bin").read_bytes(), payload)

    def test_shared_content_is_carried_once(self):
        self.store.add("demo", "x", b"same")
        self.store.add("demo", "y", b"same")
        res = self._fetch()
        self.assertEqual((self.dest / "x").read_bytes(), b"same")
        self.assertEqual((self.dest / "y").read_bytes(), b"same")
        # one blob on the wire (content-addressed), both files written
        self.assertEqual(res["fetched_blobs"], 1)
        self.assertEqual(res["written"], 2)

    def test_resync_skips_files_already_on_disk(self):
        self.store.add("demo", "a", b"one")
        self.store.add("demo", "b", b"two")
        self._fetch()
        calls_after_first = self.store.blobs2_calls
        res = self._fetch()  # nothing changed
        self.assertEqual(res["written"], 0)
        self.assertEqual(res["fetched_blobs"], 0)
        # a clean re-sync fetches no bytes; it need not call /blobs2 at all
        self.assertEqual(self.store.blobs2_calls, calls_after_first)

    def test_refused_batch_falls_back_and_still_completes(self):
        for i in range(8):
            self.store.add("demo", f"f{i}", f"body-{i}".encode() * 10)
        self.store.refuse_blobs2_over = 3  # any batch over 3 digests 502s
        res = self._fetch()
        for i in range(8):
            self.assertEqual((self.dest / f"f{i}").read_bytes(), f"body-{i}".encode() * 10)
        self.assertEqual(res["written"], 8)
        # it split past the refusal and/or dropped to single /blob — either way,
        # every file materialised
        self.assertGreater(self.store.blob_calls + self.store.blobs2_calls, 1)

    def test_hard_refusal_drops_all_the_way_to_single_blob(self):
        for i in range(5):
            self.store.add("demo", f"f{i}", f"x{i}".encode())
        self.store.refuse_blobs2_over = 0  # refuse every /blobs2 batch
        res = self._fetch()
        self.assertEqual(res["written"], 5)
        self.assertEqual(self.store.blob_calls, 5)  # one GET /blob each

    def test_unknown_digest_is_a_clear_error(self):
        self.store.add("demo", "a", b"one")
        # corrupt the manifest so it names a digest the store does not hold
        (only_path,) = list(self.store.packages["demo"])
        self.store.packages["demo"][only_path] = "0" * 64
        with self.assertRaises(kura_cli.KuraError):
            self._fetch()

    def test_bad_key_is_rejected(self):
        self.store.add("demo", "a", b"one")
        with self.assertRaises(kura_cli.KuraError):
            self._fetch(key="wrong")

    def test_prune_removes_what_the_package_no_longer_lists(self):
        self.store.add("demo", "keep.ts", b"keep")
        (self.dest / "gone").mkdir()
        (self.dest / "gone/old.ts").write_bytes(b"stale")
        (self.dest / ".provenance.json").write_bytes(b"{}")
        res = self._fetch(prune=True)
        self.assertEqual(res["pruned"], 1)
        self.assertTrue((self.dest / "keep.ts").exists())
        self.assertFalse((self.dest / "gone/old.ts").exists())
        self.assertFalse((self.dest / "gone").exists())  # and the emptied directory
        self.assertTrue((self.dest / ".provenance.json").exists())  # the consumer's own note

    def test_without_prune_a_stale_file_is_left_alone(self):
        self.store.add("demo", "keep.ts", b"keep")
        (self.dest / "old.ts").write_bytes(b"stale")
        res = self._fetch()
        self.assertNotIn("pruned", {k: v for k, v in res.items() if v})
        self.assertTrue((self.dest / "old.ts").exists())

    def test_dry_run_with_prune_reports_and_removes_nothing(self):
        self.store.add("demo", "keep.ts", b"keep")
        (self.dest / "old.ts").write_bytes(b"stale")
        res = self._fetch(prune=True, dry_run=True)
        self.assertEqual(res["would_prune"], 1)
        self.assertTrue((self.dest / "old.ts").exists())

    def test_pin_is_refused_at_the_source(self):
        self.store.add("demo", "a.ts", b"alpha")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = kura_cli.main(["fetch", "demo", str(self.dest), "--url", self.url,
                                  "--key", KEY, "--pin", "demo-abc123"])
        self.assertEqual(code, 2)
        self.assertIn("does not support pinning", err.getvalue())
        self.assertIn("do not pin packages", err.getvalue())
        self.assertFalse((self.dest / "a.ts").exists())  # nothing was fetched

    def test_no_strip_keeps_the_package_prefix(self):
        self.store.add("demo", "src/a.ts", b"alpha")
        self._fetch(strip=False)
        self.assertEqual((self.dest / "demo/src/a.ts").read_bytes(), b"alpha")


if __name__ == "__main__":
    unittest.main()
