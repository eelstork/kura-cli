"""kura board: the store's board, where agents working at once leave one
another notes (kept a day) — read what is new since you last looked, post a
note or an answer. And the first kura command run anywhere says, once, that
the board is there.

Driven against a stub store serving /board over real HTTP.
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
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kura_cli

KEY = "test-key"


class StubBoard:
    def __init__(self):
        self.notes = []

    def handler(self):
        board = self

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
                u = urlparse(self.path)
                if u.path != "/board":
                    return self._send(404, {"detail": "not found"})
                since = int(parse_qs(u.query).get("since", ["0"])[0])
                latest = board.notes[-1]["id"] if board.notes else 0
                self._send(200, {"notes": [n for n in board.notes if n["id"] > since], "latest": latest})

            def do_POST(self):
                if self.headers.get("Authorization") != f"Bearer {KEY}":
                    return self._send(401, {"detail": "no key"})
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if urlparse(self.path).path != "/board":
                    return self._send(404, {"detail": "not found"})
                if not body.get("text", "").strip():
                    return self._send(400, {"detail": "a note needs something to say"})
                note = {"id": len(board.notes) + 1, "who": body["who"], "when": "2026-10-24T12:00:00+00:00",
                        "text": body["text"], "re": body.get("re")}
                board.notes.append(note)
                self._send(200, note)

        return H


class BoardCliTest(unittest.TestCase):
    def setUp(self):
        self.board = StubBoard()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.board.handler())
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.home = tempfile.TemporaryDirectory()
        self.env = {"KURA_URL": f"http://127.0.0.1:{self.server.server_port}", "KURA_KEY": KEY,
                    "KURA_HOME": self.home.name, "KURA_WHO": "grass-agent"}
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

    def test_post_a_note_signed_by_who(self):
        code, out, _ = self.run_cli("board", "post", "changing grass packing: dark rides the colour")
        self.assertEqual(code, 0)
        self.assertEqual(self.board.notes[0]["who"], "grass-agent")
        self.assertEqual(self.board.notes[0]["text"], "changing grass packing: dark rides the colour")
        self.assertIn("#1", out)

    def test_answer_a_note(self):
        self.run_cli("board", "post", "who has grass?")
        self.run_cli("board", "post", "I do", "--re", "1")
        self.assertEqual(self.board.notes[1]["re"], 1)

    def test_read_shows_what_is_new_since_last_read(self):
        self.run_cli("board", "post", "first")
        _, out, _ = self.run_cli("board")
        self.assertIn("first", out)
        _, out, _ = self.run_cli("board")
        self.assertNotIn("first", out)
        self.assertIn("nothing new", out)
        self.run_cli("board", "post", "second")
        _, out, _ = self.run_cli("board")
        self.assertIn("second", out)
        self.assertNotIn("first", out)

    def test_all_shows_the_whole_day(self):
        self.run_cli("board", "post", "first")
        self.run_cli("board")
        _, out, _ = self.run_cli("board", "--all")
        self.assertIn("first", out)

    def test_a_refused_note_says_why(self):
        code, _, err = self.run_cli("board", "post", "   ")
        self.assertEqual(code, 1)
        self.assertIn("something to say", err)

    def test_the_first_command_says_once_that_the_board_is_there(self):
        _, _, err = self.run_cli("fetch", "nothing", self.home.name + "/out")
        self.assertIn("kura board", err)
        _, _, err = self.run_cli("fetch", "nothing", self.home.name + "/out")
        self.assertNotIn("kura board", err)

    def test_the_board_itself_needs_no_telling(self):
        _, _, err = self.run_cli("board")
        self.assertNotIn("kura board post", err)
        _, _, err = self.run_cli("fetch", "nothing", self.home.name + "/out")
        self.assertNotIn("kura board", err)


if __name__ == "__main__":
    unittest.main()
