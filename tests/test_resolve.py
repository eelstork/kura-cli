"""kura_cli.resolve, driven against a stub store that serves GET /closure.

resolve asks the store for the transitive closure of some roots and materialises
the whole set under one dest, at dest/<package>/<relpath> — the layout a
consumer aliases against. These pin the behaviour that matters: the closure is
walked, a blob shared across packages moves once, a departed dependency is
pruned, and .closure.json records the base the world was read at.
"""

import hashlib
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
    """A content-addressed store with packages, dependencies and a closure walk."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.files: dict[str, dict[str, str]] = {}   # package -> {display path -> digest}
        self.deps: dict[str, list[str]] = {}         # package -> [dependency names]
        self.roots: dict[str, str] = {}              # package -> public root
        self.base = 0

    def add(self, package, relpath, data, deps=None, root=None):
        d = _digest(data)
        self.blobs[d] = data
        self.files.setdefault(package, {})[f"{package}/{relpath}"] = d
        if deps is not None:
            self.deps[package] = deps
        if root is not None:
            self.roots[package] = root
        self.base += 1
        return d

    def closure(self, roots):
        known = set(self.files) | set(self.deps)
        seen, stack = set(), list(roots)
        while stack:
            p = stack.pop()
            if p in seen or p not in known:
                continue
            seen.add(p)
            stack.extend(self.deps.get(p, []))
        manifest: dict[str, str] = {}
        for p in sorted(seen):
            manifest.update(self.files.get(p, {}))
        return {"packages": sorted(seen), "manifest": manifest, "base": self.base,
                "roots": {p: self.roots.get(p, "src") for p in sorted(seen)}}


def _make_handler(store: StubStore):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _auth(self) -> bool:
            if self.headers.get("Authorization") == f"Bearer {KEY}":
                return True
            self._json(401, {"detail": "Missing bearer token"})
            return False

        def _json(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if not self._auth():
                return
            u = urlparse(self.path)
            if u.path == "/closure":
                roots = [r for r in parse_qs(u.query).get("roots", [""])[0].split(",") if r]
                self._json(200, store.closure(roots))
            elif u.path.startswith("/blob/"):
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
            if not self._auth():
                return
            if urlparse(self.path).path != "/blobs2":
                self._json(404, {"detail": "Not Found"})
                return
            length = int(self.headers.get("Content-Length", 0))
            digests = json.loads(self.rfile.read(length))["digests"]
            wanted = list(dict.fromkeys(digests))
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


class ResolveTest(unittest.TestCase):
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

    def _chain(self):
        # metropolis -> diarch -> {ikea -> shadelark, bigrock}
        self.store.add("shadelark", "src/raster.ts", b"raster", deps=[])
        self.store.add("bigrock", "src/rock/masonry.ts", b"masonry", deps=[])
        self.store.add("ikea", "src/prop.ts", b"prop", deps=["shadelark"])
        self.store.add("diarch", "src/f.ts", b"f", deps=["ikea", "bigrock"])
        self.store.add("metropolis", "src/town.ts", b"town", deps=["diarch"])

    def _resolve(self, *roots, **kw):
        kw.setdefault("url", self.url)
        kw.setdefault("key", KEY)
        return kura_cli.resolve(self.dest, list(roots), **kw)

    def test_materialises_the_whole_closure_under_package_dirs(self):
        self._chain()
        res = self._resolve("metropolis")
        self.assertEqual(set(res["packages"]), {"metropolis", "diarch", "ikea", "shadelark", "bigrock"})
        self.assertEqual((self.dest / "shadelark/src/raster.ts").read_bytes(), b"raster")
        self.assertEqual((self.dest / "metropolis/src/town.ts").read_bytes(), b"town")
        self.assertEqual((self.dest / "bigrock/src/rock/masonry.ts").read_bytes(), b"masonry")

    def test_writes_a_closure_lockfile_with_the_base(self):
        self._chain()
        self._resolve("metropolis")
        lock = json.loads((self.dest / ".closure.json").read_text())
        self.assertEqual(lock["requested"], ["metropolis"])   # the roots asked for
        self.assertEqual(set(lock["packages"]), {"metropolis", "diarch", "ikea", "shadelark", "bigrock"})
        self.assertEqual(lock["base"], self.store.base)
        self.assertEqual(set(lock["roots"]), set(lock["packages"]))   # per-package root map

    def test_a_blob_shared_across_packages_moves_once(self):
        self.store.add("a", "x.ts", b"same", deps=[])
        self.store.add("b", "y.ts", b"same", deps=["a"])
        res = self._resolve("b")
        self.assertEqual(res["fetched_blobs"], 1)
        self.assertEqual((self.dest / "a/x.ts").read_bytes(), b"same")
        self.assertEqual((self.dest / "b/y.ts").read_bytes(), b"same")

    def test_reresolve_skips_what_is_already_present(self):
        self._chain()
        self._resolve("metropolis")
        res = self._resolve("metropolis")
        self.assertEqual(res["written"], 0)
        self.assertEqual(res["fetched_blobs"], 0)

    def test_a_departed_dependency_is_pruned(self):
        self._chain()
        self._resolve("metropolis")
        self.assertTrue((self.dest / "bigrock/src/rock/masonry.ts").exists())
        self.store.deps["diarch"] = ["ikea"]   # diarch drops bigrock
        self.store.base += 1
        res = self._resolve("metropolis")
        self.assertFalse((self.dest / "bigrock").exists())   # files and the emptied dir
        self.assertGreaterEqual(res["pruned"], 1)
        lock = json.loads((self.dest / ".closure.json").read_text())
        self.assertNotIn("bigrock", lock["packages"])

    def test_an_unresolved_root_refuses_and_does_not_wipe_the_tree(self):
        # a typo'd or unpublished root returns an empty/partial closure from the
        # store; resolve must refuse rather than prune a populated ext/ to nothing
        self._chain()
        self._resolve("metropolis")
        before = sorted(p.name for p in (self.dest / "metropolis").rglob("*"))
        with self.assertRaises(kura_cli.KuraError):
            self._resolve("metropoliss")          # one-letter typo -> unknown root
        self.assertTrue((self.dest / "metropolis/src/town.ts").exists())  # nothing wiped
        self.assertTrue((self.dest / "shadelark/src/raster.ts").exists())
        self.assertEqual(sorted(p.name for p in (self.dest / "metropolis").rglob("*")), before)

    def test_a_partly_unresolved_root_set_also_refuses(self):
        self._chain()
        self._resolve("metropolis")
        with self.assertRaises(kura_cli.KuraError):
            self._resolve("metropolis", "nope")   # one good, one unknown
        self.assertTrue((self.dest / "metropolis/src/town.ts").exists())

    def test_the_lockfile_is_not_pruned_on_a_reresolve(self):
        self._chain()
        self._resolve("metropolis")
        self._resolve("metropolis")
        self.assertTrue((self.dest / ".closure.json").exists())

    def test_dry_run_writes_nothing(self):
        self._chain()
        res = self._resolve("metropolis", dry_run=True)
        self.assertEqual(res["would_fetch_blobs"], 5)
        self.assertFalse((self.dest / "metropolis").exists())
        self.assertFalse((self.dest / ".closure.json").exists())

    def test_bad_key_is_rejected(self):
        self._chain()
        with self.assertRaises(kura_cli.KuraError):
            self._resolve("metropolis", key="wrong")

    def test_cli_resolve_end_to_end(self):
        self._chain()
        code = kura_cli.main(["resolve", str(self.dest), "metropolis",
                              "--url", self.url, "--key", KEY, "--quiet"])
        self.assertEqual(code, 0)
        self.assertTrue((self.dest / "ikea/src/prop.ts").exists())


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.store = StubStore()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self.store))
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        host, port = self.httpd.server_address
        self.url = f"http://{host}:{port}"
        self._dir = tempfile.TemporaryDirectory()
        self.repo = Path(self._dir.name)
        self.dest = self.repo / "ext"   # dest is <repo>/ext by convention

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self._dir.cleanup()

    def _sync(self, *roots, **kw):
        kw.setdefault("url", self.url)
        kw.setdefault("key", KEY)
        return kura_cli.sync(self.dest, list(roots), **kw)

    def test_sync_writes_the_alias_map_and_tsconfig_paths_from_the_store_roots(self):
        self.store.add("shadelark", "src/raster.ts", b"raster", deps=[])
        self.store.add("bigrock", "src/rock/masonry.ts", b"masonry", deps=[], root="src/rock")
        self.store.add("ikea", "src/index.ts", b"export const e = 1;", deps=["shadelark", "bigrock"])
        self._sync("ikea")

        aliases = json.loads((self.dest / ".aliases.json").read_text())
        self.assertEqual(aliases["@shadelark"], "ext/shadelark/src")
        self.assertEqual(aliases["@bigrock"], "ext/bigrock/src/rock")   # non-default root honoured
        self.assertEqual(aliases["@ikea"], "ext/ikea/src")

        paths = json.loads((self.repo / "tsconfig.paths.json").read_text())["compilerOptions"]["paths"]
        self.assertEqual(paths["@bigrock/*"], ["ext/bigrock/src/rock/*"])
        self.assertEqual(paths["@ikea"], ["ext/ikea/src/index.ts"])    # bare path where index exists
        self.assertNotIn("@shadelark", paths)                          # no index.ts -> no bare path
        self.assertEqual(paths["@shadelark/*"], ["ext/shadelark/src/*"])

    def test_sync_materialises_the_tree_too(self):
        self.store.add("shadelark", "src/raster.ts", b"raster", deps=[])
        self.store.add("ikea", "src/index.ts", b"e", deps=["shadelark"])
        self._sync("ikea")
        self.assertEqual((self.dest / "shadelark/src/raster.ts").read_bytes(), b"raster")
        self.assertTrue((self.dest / ".closure.json").exists())

    def test_sync_refuses_an_unresolved_root(self):
        self.store.add("ikea", "src/index.ts", b"e", deps=[])
        with self.assertRaises(kura_cli.KuraError):
            self._sync("ikeaa")   # typo -> refuse, write no harness
        self.assertFalse((self.repo / "tsconfig.paths.json").exists())


if __name__ == "__main__":
    unittest.main()
