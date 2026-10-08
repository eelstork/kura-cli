"""kura_cli on a tag: publish where only builds on the tag look, and build
through the tag — driven against a stub store that serves tags over real HTTP.

A tag lets an agent publish its packages without landing them on main, then
build its app with those packages and everyone else's main. What matters here:
publish lands on the tag named (by --tag or KURA_TAG), sync/resolve/fetch read
through it and record it, and — the one dangerous direction — a store too old to
know tags is refused rather than silently served main (or, for a publish,
silently written to main).
"""

import base64
import hashlib
import io
import json
import os
import struct
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kura_cli

KEY = "test-key"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StubStore:
    """Main plus tag overlays, with the reads and the publish a session uses.
    `knows_tags=False` plays a store from before tags: it ignores `tag`
    wherever it appears, exactly as an older kura would."""

    def __init__(self, knows_tags=True):
        self.knows_tags = knows_tags
        self.blobs: dict[str, bytes] = {}
        self.files: dict[str, dict[str, str]] = {}                 # main: package -> {path: digest}
        self.deps: dict[str, list[str]] = {}
        self.base = 0
        self.tag_files: dict[str, dict[str, dict[str, str]]] = {}  # tag -> package -> {path: digest}
        self.tag_deps: dict[str, dict[str, list[str]]] = {}
        self.tag_base: dict[str, int] = {}
        self.publishes: list[dict] = []   # every publish body received
        self.published: dict = {}         # (tag, package) -> its latest landing, as kura reports it
        self.races = 0                    # answer this many publishes with 409 first

    def _put(self, data):
        d = _digest(data)
        self.blobs[d] = data
        return d

    def add(self, package, relpath, data, deps=None, tag=None):
        d = self._put(data)
        if tag is None:
            self.files.setdefault(package, {})[f"{package}/{relpath}"] = d
            if deps is not None:
                self.deps[package] = deps
            self.base += 1
        else:
            self.tag_files.setdefault(tag, {}).setdefault(package, {})[f"{package}/{relpath}"] = d
            if deps is not None:
                self.tag_deps.setdefault(tag, {})[package] = deps
            self.tag_base[tag] = self.tag_base.get(tag, 0) + 1
        return d

    def _tag(self, value):
        if not self.knows_tags or value in (None, "", "main"):
            return None
        return value

    def package_on(self, name, tag):
        carried = self.tag_files.get(tag, {}) if tag else {}
        return dict(carried.get(name) or self.files.get(name, {}))

    def closure(self, roots, tag):
        carried = self.tag_files.get(tag, {}) if tag else {}
        known = set(self.files) | set(self.deps) | set(carried)
        seen, stack = set(), list(roots)
        while stack:
            p = stack.pop()
            if p in seen or p not in known:
                continue
            seen.add(p)
            deps = self.tag_deps.get(tag, {}).get(p, []) if p in carried else self.deps.get(p, [])
            stack.extend(deps)
        manifest = {}
        for p in sorted(seen):
            manifest.update(self.package_on(p, tag))
        out = {"packages": sorted(seen), "manifest": manifest, "base": self.base,
               "roots": {p: "src" for p in sorted(seen)},
               "published": {p: self.published[(tag if p in carried else None, p)] for p in sorted(seen)
                             if (tag if p in carried else None, p) in self.published}}
        if tag:
            out.update(tag=tag, tagged=sorted(seen & set(carried)), tag_base=self.tag_base.get(tag, 0))
        return out

    def publish(self, name, body):
        self.publishes.append(body)
        if self.races:
            self.races -= 1
            return 409, {"detail": {"conflict": [f"{name}/raced.ts"]}}
        tag = self._tag(body.get("tag"))
        if not 12 <= len(body["message"].strip()) <= 256:
            return 400, {"detail": {"message": "commit message must be 12–256 characters"}}
        current = self.tag_base.get(tag, 0) if tag else self.base
        if body["base"] != current:
            return 409, {"detail": {"conflict": [f"{name}/x"]}}
        tree = {f"{name}/{rel}": self._put(base64.b64decode(b)) for rel, b in body["files"].items()}
        if tag:
            before = self.tag_files.setdefault(tag, {}).get(name, {})
            self.tag_files[tag][name] = tree
            self.tag_base[tag] = current + 1
        else:
            before = self.files.get(name, {})
            self.files[name] = tree
            self.base += 1
        self.published[(tag, name)] = {"seq": current, "who": body["who"], "when": "now",
                                        "message": body["message"], "source": body.get("source")}
        out = {"seq": current, "written": sum(1 for p, d in tree.items() if before.get(p) != d),
               "buried": sorted(p for p in before if p not in tree)}
        if tag:
            out["tag"] = tag
        return 200, out


def _make_handler(store: StubStore):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _auth(self):
            if self.headers.get("Authorization") == f"Bearer {KEY}":
                return True
            self._json(401, {"detail": "Missing bearer token"})
            return False

        def do_GET(self):
            if not self._auth():
                return
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
            raw_tag = q.get("tag")
            if store.knows_tags and raw_tag == "Fern":
                self._json(400, {"detail": {"message": "tag 'Fern' is not a valid tag"}})
                return
            tag = store._tag(raw_tag)
            if u.path == "/base":
                self._json(200, {"base": store.tag_base.get(tag, 0), "tag": tag} if tag else {"base": store.base})
            elif u.path == "/manifest":
                self._json(200, store.package_on(q["package"], tag))
            elif u.path == "/closure":
                self._json(200, store.closure([r for r in q.get("roots", "").split(",") if r], tag))
            elif u.path.startswith("/blob/"):
                data = store.blobs[u.path[len("/blob/"):]]
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._json(404, {"detail": "Not Found"})

        def do_POST(self):
            if not self._auth():
                return
            path = urlparse(self.path).path
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            if path.startswith("/packages/") and path.endswith("/publish"):
                code, out = store.publish(path.split("/")[2], body)
                self._json(code, out)
                return
            if path != "/blobs2":
                self._json(404, {"detail": "Not Found"})
                return
            wanted = list(dict.fromkeys(body["digests"]))
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            for d in wanted:
                data = store.blobs[d]
                head = d.encode("ascii")
                self.wfile.write(struct.pack(">I", len(head)) + head + struct.pack(">Q", len(data)) + data)

    return Handler


class TagTest(unittest.TestCase):
    knows_tags = True

    def setUp(self):
        self.store = StubStore(knows_tags=self.knows_tags)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self.store))
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        host, port = self.httpd.server_address
        self.url = f"http://{host}:{port}"
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self._env = {k: os.environ.pop(k, None) for k in ("KURA_TAG", "KURA_WHO")}
        self._backoff = kura_cli.RACE_BACKOFF
        kura_cli.RACE_BACKOFF = (0, 0, 0)

    def tearDown(self):
        kura_cli.RACE_BACKOFF = self._backoff
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.httpd.shutdown()
        self.httpd.server_close()
        self._tmp.cleanup()

    def _world(self):
        """shadelark <- ikea <- diarch on main; ikea changed on tag fern."""
        self.store.add("shadelark", "src/raster.ts", b"raster")
        self.store.add("ikea", "src/prop.ts", b"main ikea", deps=["shadelark"])
        self.store.add("diarch", "src/arch.ts", b"main diarch", deps=["ikea"])
        self.store.add("ikea", "src/prop.ts", b"fern ikea", deps=["shadelark"], tag="fern")

    def _src(self, files):
        root = self.tmp / "repo"
        for rel, data in files.items():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        return root

    def _main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = kura_cli.main([*argv, "--url", self.url, "--key", KEY])
        return code, out.getvalue(), err.getvalue()


class TestPublish(TagTest):
    def test_publish_lands_the_tree_on_the_tag(self):
        src = self._src({"src/a.ts": b"a", "src/deep/b.ts": b"b", "README.md": b"r"})
        res = kura_cli.publish("core", src, ["src"], message="core: try a new a", tag="fern",
                               url=self.url, key=KEY)
        sent = self.store.publishes[-1]
        self.assertEqual(sent["tag"], "fern")
        self.assertEqual(sent["base"], 0)  # the tag's own base
        self.assertEqual({p: base64.b64decode(b) for p, b in sent["files"].items()},
                         {"src/a.ts": b"a", "src/deep/b.ts": b"b"})
        self.assertEqual(res["tag"], "fern")
        self.assertEqual(res["files"], 2)
        self.assertEqual(self.store.files, {})  # main untouched

    def test_a_path_may_name_one_file(self):
        src = self._src({"src/view/bend.ts": b"bend", "src/view/stage.ts": b"stage"})
        kura_cli.publish("metropolis", src, ["src/view/bend.ts"], message="metropolis: bend only",
                         tag="fern", url=self.url, key=KEY)
        self.assertEqual(list(self.store.publishes[-1]["files"]), ["src/view/bend.ts"])

    def test_the_whole_dir_leaves_out_what_is_never_package_content(self):
        src = self._src({"src/a.ts": b"a", ".git/HEAD": b"ref", "node_modules/x/i.js": b"x",
                         "src/__pycache__/m.pyc": b"c"})
        kura_cli.publish("core", src, [], message="core: the whole tree", tag="fern", url=self.url, key=KEY)
        self.assertEqual(list(self.store.publishes[-1]["files"]), ["src/a.ts"])

    def test_a_missing_path_is_refused_before_anything_is_sent(self):
        src = self._src({"src/a.ts": b"a"})
        with self.assertRaises(kura_cli.KuraError) as e:
            kura_cli.publish("core", src, ["src/nope"], message="core: a missing path", tag="fern",
                             url=self.url, key=KEY)
        self.assertIn("src/nope", str(e.exception))
        self.assertEqual(self.store.publishes, [])

    def test_nothing_to_publish_is_refused(self):
        src = self._src({".git/HEAD": b"ref"})
        with self.assertRaises(kura_cli.KuraError):
            kura_cli.publish("core", src, [], message="core: nothing at all", tag="fern", url=self.url, key=KEY)
        self.assertEqual(self.store.publishes, [])

    def test_without_a_tag_it_publishes_on_main(self):
        self.store.add("core", "old.ts", b"old")
        src = self._src({"src/a.ts": b"a"})
        res = kura_cli.publish("core", src, ["src"], message="core: straight to main", url=self.url, key=KEY)
        sent = self.store.publishes[-1]
        self.assertNotIn("tag", sent)
        self.assertEqual(sent["base"], 1)
        self.assertIsNone(res["tag"])
        self.assertEqual(res["buried"], ["core/old.ts"])

    def test_main_named_as_the_tag_is_main(self):
        src = self._src({"src/a.ts": b"a"})
        kura_cli.publish("core", src, ["src"], message="core: straight to main", tag="main", url=self.url, key=KEY)
        self.assertNotIn("tag", self.store.publishes[-1])

    def test_KURA_TAG_picks_the_tag_for_the_session(self):
        os.environ["KURA_TAG"] = "fern"
        src = self._src({"src/a.ts": b"a"})
        code, out, err = self._main("publish", "core", str(src), "src", "-m", "core: on the session tag")
        self.assertEqual(code, 0, err)
        self.assertEqual(self.store.publishes[-1]["tag"], "fern")
        self.assertIn("on tag fern", out)

    def test_the_flag_beats_KURA_TAG(self):
        os.environ["KURA_TAG"] = "fern"
        src = self._src({"src/a.ts": b"a"})
        code, out, _ = self._main("publish", "core", str(src), "src", "-m", "core: straight to main", "--tag", "main")
        self.assertEqual(code, 0)
        self.assertNotIn("tag", self.store.publishes[-1])
        self.assertIn("on main", out)

    def test_who_comes_from_the_flag_or_KURA_WHO(self):
        src = self._src({"src/a.ts": b"a"})
        os.environ["KURA_WHO"] = "agent-fern"
        kura_cli.publish("core", src, ["src"], message="core: signed by env", tag="fern", url=self.url, key=KEY)
        self.assertEqual(self.store.publishes[-1]["who"], "agent-fern")
        kura_cli.publish("core", src, ["src"], message="core: signed by flag", tag="fern", who="me",
                         url=self.url, key=KEY)
        self.assertEqual(self.store.publishes[-1]["who"], "me")

    def test_a_raced_base_is_re_read_and_retried(self):
        self.store.races = 2
        src = self._src({"src/a.ts": b"a"})
        res = kura_cli.publish("core", src, ["src"], message="core: after two races", tag="fern",
                               url=self.url, key=KEY)
        self.assertEqual(len(self.store.publishes), 3)
        self.assertEqual(res["seq"], 0)

    def test_the_stores_refusal_is_reported(self):
        src = self._src({"src/a.ts": b"a"})
        with self.assertRaises(kura_cli.KuraError) as e:
            kura_cli.publish("core", src, ["src"], message="short", tag="fern", url=self.url, key=KEY)
        self.assertIn("12–256", str(e.exception))

    def test_a_bad_tag_is_the_stores_to_refuse(self):
        src = self._src({"src/a.ts": b"a"})
        with self.assertRaises(kura_cli.KuraError) as e:
            kura_cli.publish("core", src, ["src"], message="core: a shouty tag", tag="Fern", url=self.url, key=KEY)
        self.assertIn("not a valid tag", str(e.exception))
        self.assertEqual(self.store.publishes, [])

    def test_dry_run_sends_nothing(self):
        src = self._src({"src/a.ts": b"a", "src/b.ts": b"bb"})
        code, out, _ = self._main("publish", "core", str(src), "src", "-m", "core: just looking", "--tag", "fern",
                                  "--dry-run")
        self.assertEqual(code, 0)
        self.assertEqual(self.store.publishes, [])
        self.assertIn("2 file(s)", out)

    def test_a_message_is_required(self):
        src = self._src({"src/a.ts": b"a"})
        with self.assertRaises(SystemExit):
            with redirect_stderr(io.StringIO()):
                kura_cli.main(["publish", "core", str(src), "src", "--url", self.url, "--key", KEY])


class TestBuildingOnATag(TagTest):
    def test_sync_takes_tagged_packages_from_the_tag_and_the_rest_from_main(self):
        self._world()
        dest = self.tmp / "ext"
        res = kura_cli.sync(dest, ["diarch"], url=self.url, key=KEY, tag="fern")
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"fern ikea")
        self.assertEqual((dest / "diarch/src/arch.ts").read_bytes(), b"main diarch")
        self.assertEqual(res["tag"], "fern")
        self.assertEqual(res["tagged"], ["ikea"])
        lock = json.loads((dest / ".closure.json").read_text())
        self.assertEqual((lock["tag"], lock["tagged"], lock["tag_base"]), ("fern", ["ikea"], 1))

    def test_a_tag_nothing_is_published_on_builds_all_of_main(self):
        self._world()
        dest = self.tmp / "ext"
        res = kura_cli.resolve(dest, ["diarch"], url=self.url, key=KEY, tag="moss")
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"main ikea")
        self.assertEqual(res["tagged"], [])

    def test_KURA_TAG_is_what_a_build_passes_in(self):
        # a Dockerfile's `ARG KURA_TAG` reaches the fetch step as this variable
        self._world()
        os.environ["KURA_TAG"] = "fern"
        dest = self.tmp / "ext"
        code, out, err = self._main("sync", str(dest), "diarch")
        self.assertEqual(code, 0, err)
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"fern ikea")
        self.assertIn("on tag fern", out)
        self.assertIn("ikea", out)

    def test_an_empty_KURA_TAG_is_main(self):
        # an ARG declared but not passed arrives empty
        self._world()
        os.environ["KURA_TAG"] = ""
        dest = self.tmp / "ext"
        code, _, err = self._main("sync", str(dest), "diarch")
        self.assertEqual(code, 0, err)
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"main ikea")
        self.assertNotIn("tag", json.loads((dest / ".closure.json").read_text()))

    def test_a_main_build_after_a_tag_build_puts_main_back(self):
        self._world()
        dest = self.tmp / "ext"
        kura_cli.sync(dest, ["diarch"], url=self.url, key=KEY, tag="fern")
        kura_cli.sync(dest, ["diarch"], url=self.url, key=KEY)
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"main ikea")
        self.assertNotIn("tag", json.loads((dest / ".closure.json").read_text()))

    def test_fetch_one_package_through_the_tag(self):
        self._world()
        dest = self.tmp / "ikea"
        kura_cli.fetch("ikea", dest, url=self.url, key=KEY, tag="fern")
        self.assertEqual((dest / "src/prop.ts").read_bytes(), b"fern ikea")
        kura_cli.fetch("diarch", self.tmp / "diarch", url=self.url, key=KEY, tag="fern")
        self.assertEqual((self.tmp / "diarch/src/arch.ts").read_bytes(), b"main diarch")


class TestAStoreThatDoesNotKnowTags(TagTest):
    """An older kura ignores `tag`: it would serve main to a tag build and write
    a tag publish to main. Both are refused, before anything moves."""

    knows_tags = False

    def test_publish_is_refused_and_nothing_is_sent(self):
        src = self._src({"src/a.ts": b"a"})
        with self.assertRaises(kura_cli.KuraError) as e:
            kura_cli.publish("core", src, ["src"], message="core: meant for a tag", tag="fern",
                             url=self.url, key=KEY)
        self.assertIn("does not know tags", str(e.exception))
        self.assertEqual(self.store.publishes, [])

    def test_sync_on_a_tag_is_refused_and_the_tree_is_left_alone(self):
        self._world()
        dest = self.tmp / "ext"
        (dest / "keep").mkdir(parents=True)
        (dest / "keep/me.ts").write_bytes(b"mine")
        with self.assertRaises(kura_cli.KuraError) as e:
            kura_cli.sync(dest, ["diarch"], url=self.url, key=KEY, tag="fern")
        self.assertIn("does not know tags", str(e.exception))
        self.assertEqual([p.name for p in dest.rglob("*") if p.is_file()], ["me.ts"])

    def test_fetch_on_a_tag_is_refused(self):
        self._world()
        with self.assertRaises(kura_cli.KuraError):
            kura_cli.fetch("ikea", self.tmp / "ikea", url=self.url, key=KEY, tag="fern")

    def test_main_still_works_against_it(self):
        self._world()
        dest = self.tmp / "ext"
        kura_cli.sync(dest, ["diarch"], url=self.url, key=KEY)
        self.assertEqual((dest / "ikea/src/prop.ts").read_bytes(), b"main ikea")


if __name__ == "__main__":
    unittest.main()
