"""kura archive: archive a package. The store keeps it but no longer hands it
out, so a build asking for it is refused with the store's own words — that it
is archived, why, and which packages draw it in — and whoever wants it learns
to use something else.

Driven against a stub store serving POST /packages/{name}/archive, and
GET /closure and GET /manifest refusing an archived package, over real HTTP.
"""

import io
import json
import os
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kura_cli

KEY = "test-key"
REFUSAL = ("shadelark is archived (folded into planetoid), drawn in by planetoid "
           "— kura no longer hands it out: use something else")


class StubStore:
    def __init__(self):
        self.archived = {}
        self.packages = {"shadelark": ["planetoid"], "arena": []}  # name -> live dependents

    def handler(self):
        store = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.headers.get("Authorization") != f"Bearer {KEY}":
                    return self._send(401, {"detail": "no key"})
                if urlparse(self.path).path == "/manifest":
                    return self._send(410, {"detail": {"message": "arena is archived (use proshow) — kura no "
                                                                  "longer hands it out: use something else",
                                                       "archived": ["arena"]}})
                if urlparse(self.path).path == "/closure":
                    return self._send(410, {"detail": {"message": REFUSAL, "archived": ["shadelark"],
                                                       "via": {"shadelark": ["planetoid"]}}})
                self._send(404, {"detail": "not found"})

            def do_POST(self):
                if self.headers.get("Authorization") != f"Bearer {KEY}":
                    return self._send(401, {"detail": "no key"})
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
                parts = urlparse(self.path).path.strip("/").split("/")
                if len(parts) != 3 or parts[0] != "packages" or parts[2] != "archive":
                    return self._send(404, {"detail": "not found"})
                name = parts[1]
                if name not in store.packages:
                    return self._send(404, {"detail": f"no such package: {name}"})
                info = store.archived.setdefault(name, {"who": body.get("who") or "key",
                                                        "when": "2026-11-15T12:00:00+00:00",
                                                        "reason": body.get("reason", "")})
                self._send(200, {"name": name, "archived": info, "used_by": store.packages[name]})

        return H


class ArchiveCliTest(unittest.TestCase):
    def setUp(self):
        self.store = StubStore()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.store.handler())
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.home = tempfile.TemporaryDirectory()
        self.env = {"KURA_URL": f"http://127.0.0.1:{self.server.server_port}", "KURA_KEY": KEY,
                    "KURA_HOME": self.home.name, "KURA_WHO": "eve@main"}
        self._old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.server.shutdown()
        self.home.cleanup()

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = kura_cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_it_archives_with_why_and_who(self):
        code, out, _ = self.run_cli("archive", "arena", "--reason", "use proshow instead")
        self.assertEqual(code, 0)
        self.assertEqual(self.store.archived["arena"]["reason"], "use proshow instead")
        self.assertEqual(self.store.archived["arena"]["who"], "eve@main")
        self.assertIn("archived arena", out)

    def test_who_can_be_said(self):
        self.run_cli("archive", "arena", "--who", "the-gardener")
        self.assertEqual(self.store.archived["arena"]["who"], "the-gardener")

    def test_it_names_the_packages_still_using_it(self):
        _, out, _ = self.run_cli("archive", "shadelark")
        self.assertIn("still used by planetoid", out)

    def test_an_unknown_package_says_so(self):
        code, _, err = self.run_cli("archive", "nowhere")
        self.assertEqual(code, 1)
        self.assertIn("no such package: nowhere", err)

    def test_a_wrong_key_says_so(self):
        code, _, err = self.run_cli("archive", "arena", "--key", "wrong")
        self.assertEqual(code, 1)
        self.assertIn("KURA_KEY", err)

    def test_fetching_an_archived_package_is_told_in_the_stores_words(self):
        code, _, err = self.run_cli("fetch", "arena", self.home.name + "/out")
        self.assertEqual(code, 1)
        self.assertIn("arena is archived (use proshow)", err)

    def test_a_build_reaching_an_archived_package_is_told_in_the_stores_words(self):
        code, _, err = self.run_cli("resolve", self.home.name + "/ext", "planetoid")
        self.assertEqual(code, 1)
        self.assertIn("shadelark is archived (folded into planetoid), drawn in by planetoid", err)


if __name__ == "__main__":
    unittest.main()
