"""kura_cli publish says where a package came from, and sync records it.

A publish reads git in the directory it publishes — the repo (its origin, as
owner/name), the branch and the commit — and sends that as the publish's
source, so the store can say where each package came from. The repo is named,
never addressed: an origin URL that carries a token must not leave the
machine. A directory that is not in git publishes with no source, as before.

On the reading side, sync/resolve keep the store's word on where each package
came from (`published` on the closure) in `.closure.json`, beside the base and
the tag — a build's record of what it drew.
"""

import os
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kura_cli
from test_tags import TagTest

import json


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class SourceTest(TagTest):
    def _repo(self, origin="https://github.com/eelstork/planetoid.git", branch="main"):
        root = self._src({"src/a.ts": b"a", "src/b.ts": b"b"})
        _git(root, "init", "-q", "-b", branch)
        _git(root, "-c", "user.name=t", "-c", "user.email=t@t", "add", ".")
        _git(root, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "first")
        if origin:
            _git(root, "remote", "add", "origin", origin)
        return root

    def _sent(self):
        return self.store.publishes[-1].get("source")


class TestPublishSendsItsSource(SourceTest):
    def test_repo_branch_and_commit_are_read_from_git(self):
        root = self._repo()
        code, out, _ = self._main("publish", "planetoid", str(root), "src", "-m", "publish the engine core")
        self.assertEqual(code, 0)
        self.assertEqual(self._sent(), {"repo": "eelstork/planetoid", "branch": "main",
                                        "commit": _git(root, "rev-parse", "HEAD")})
        self.assertIn("eelstork/planetoid", out)

    def test_a_token_in_the_origin_never_leaves(self):
        root = self._repo(origin="https://x-access-token:n0t-a-real-t0ken@github.com/eelstork/planetoid")
        self._main("publish", "planetoid", str(root), "-m", "publish the engine core")
        sent = json.dumps(self.store.publishes[-1])
        self.assertNotIn("n0t-a-real-t0ken", sent)
        self.assertNotIn("x-access-token", sent)
        self.assertEqual(self._sent()["repo"], "eelstork/planetoid")

    def test_the_repo_is_named_whatever_form_the_origin_takes(self):
        for origin in ("git@github.com:eelstork/planetoid.git",
                       "ssh://git@github.com/eelstork/planetoid",
                       "http://local_proxy@127.0.0.1:41733/git/eelstork/planetoid"):
            self.assertEqual(kura_cli._repo_name(origin), "eelstork/planetoid", origin)

    def test_a_detached_head_sends_no_branch(self):
        root = self._repo()
        _git(root, "checkout", "-q", "--detach")
        self._main("publish", "planetoid", str(root), "-m", "publish the engine core")
        self.assertNotIn("branch", self._sent())
        self.assertIn("commit", self._sent())

    def test_a_repo_with_no_origin_still_sends_branch_and_commit(self):
        root = self._repo(origin=None)
        self._main("publish", "planetoid", str(root), "-m", "publish the engine core")
        self.assertEqual(set(self._sent()), {"branch", "commit"})

    def test_a_directory_not_in_git_sends_no_source(self):
        root = self._src({"a.ts": b"a"})
        code, _, _ = self._main("publish", "core", str(root), "-m", "publish without git")
        self.assertEqual(code, 0)
        self.assertNotIn("source", self.store.publishes[-1])

    def test_the_publisher_may_say_it_themselves(self):
        root = self._repo()
        self._main("publish", "planetoid", str(root), "-m", "publish the engine core",
                   "--source-repo", "eelstork/elsewhere", "--source-branch", "claude/fern")
        sent = self._sent()
        self.assertEqual((sent["repo"], sent["branch"]), ("eelstork/elsewhere", "claude/fern"))
        self.assertEqual(sent["commit"], _git(root, "rev-parse", "HEAD"))

    def test_no_source_sends_none(self):
        root = self._repo()
        self._main("publish", "planetoid", str(root), "-m", "publish the engine core", "--no-source")
        self.assertNotIn("source", self.store.publishes[-1])

    def test_a_dry_run_says_the_source_it_would_send(self):
        root = self._repo()
        res = kura_cli.publish("planetoid", root, message="publish the engine core",
                               url=self.url, key="test-key", dry_run=True)
        self.assertEqual(res["source"]["repo"], "eelstork/planetoid")
        self.assertEqual(self.store.publishes, [])


class TestSyncRecordsWhereEachPackageCameFrom(SourceTest):
    def test_closure_json_keeps_the_stores_published_records(self):
        root = self._repo()
        self._main("publish", "planetoid", str(root), "src", "-m", "publish the engine core")
        code, _, _ = self._main("sync", str(self.tmp / "app" / "ext"), "planetoid")
        self.assertEqual(code, 0)
        lock = json.loads((self.tmp / "app" / "ext" / ".closure.json").read_text())
        self.assertEqual(lock["published"]["planetoid"]["source"]["repo"], "eelstork/planetoid")

    def test_a_store_that_reports_no_publishes_records_none(self):
        self.store.add("core", "src/a.ts", b"a")
        self._main("sync", str(self.tmp / "app" / "ext"), "core")
        lock = json.loads((self.tmp / "app" / "ext" / ".closure.json").read_text())
        self.assertEqual(lock["published"], {})


if __name__ == "__main__":
    unittest.main()
