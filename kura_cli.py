#!/usr/bin/env python3
"""kura-cli — fetch a package's files out of a kura store.

A kura store keeps files content-addressed and hands them out in two steps:
GET /manifest?package=<name> gives the tree as path -> digest, and POST /blobs2
streams the bytes for a list of digests. This tool walks that loop for you:
read the manifest, work out which digests are not already on disk, pull them in
one streamed call, and write the tree out. Because the store is
content-addressed, a re-fetch of an unchanged package moves no bytes.

The stream is raw and framed, a blob at a time, so a large package neither
inflates over the wire nor stands in memory whole. If a batch is ever refused
(an old or overloaded store), the fetch splits it and, in the last resort, falls
back to one GET /blob per digest — a package never fails whole on a single
refused batch.

Usage:
    kura fetch   <package> <dest> [--tag TAG] [--no-strip] [--prune] [--dry-run]
    kura resolve <dest> <root>... [--tag TAG] [--no-prune] [--dry-run]
    kura sync    <dest> <root>... [--tag TAG] [--no-prune] [--dry-run]
    kura publish <package> <dir> [<path>...] -m MESSAGE [--tag TAG] [--who WHO] [--dry-run]
                 [--source-repo R] [--source-branch B] [--source-commit C] [--no-source]

    KURA_URL   store base URL   (default https://kura-staging.fly.dev)
    KURA_KEY   bearer token     (required unless --key is given)
    KURA_TAG   the tag to read and publish on (default: none, i.e. main)
    KURA_WHO   who a publish is signed by (default kura-cli)

A tag is where an agent publishes packages without landing them on main: a
build on the tag reads each package the tag carries from the tag, and every
other package from main. Pick one tag for a piece of work, set KURA_TAG, and
publish and build on it; `main` (or an empty tag) means main.

A publish says where its tree came from: the repo (named owner/name, never its
address), the branch and the commit, read from git in the published directory.
sync and resolve keep the store's record of where each package came from in
`.closure.json`.

Speaks only the Python standard library, so a session can vendor this one file
and run it with nothing to install.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_URL = "https://kura-staging.fly.dev"
BACKOFF = (2, 4, 8)  # seconds between retries of a transient network failure
RACE_BACKOFF = (1, 2, 4)  # seconds before re-reading a base another publish moved
TIMEOUT = 300


class KuraError(Exception):
    """A fetch cannot proceed: a bad key, a store that lacks the endpoint, a
    manifest naming bytes the store does not hold, a digest that arrives wrong."""


class _MissingDigests(KuraError):
    def __init__(self, digests):
        self.digests = list(digests)
        super().__init__(f"the store is missing {len(self.digests)} digest(s) the manifest names: "
                         f"{', '.join(self.digests[:4])}{' ...' if len(self.digests) > 4 else ''}")


class _BatchRefused(Exception):
    """A /blobs2 batch did not come back (a 5xx, or the connection dropped) —
    the reply may be too big for the store, so the caller splits or falls back.
    Not a KuraError: it is recoverable."""


# --- HTTP ------------------------------------------------------------------

def _request(url, token, data=None, timeout=TIMEOUT):
    headers = {"Authorization": f"Bearer {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    method = "POST" if data is not None else "GET"
    return urllib.request.urlopen(
        urllib.request.Request(url, data=data, method=method, headers=headers),
        timeout=timeout,
    )


def _get_json(base, path, token):
    try:
        with _request(base + path, token) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise KuraError("the store rejected the key (401) — check KURA_KEY")
        why = _detail_text(e)
        raise KuraError(f"GET {path} -> HTTP {e.code}{': ' + why if why else ''}")
    except (urllib.error.URLError, OSError) as e:
        raise KuraError(f"GET {path} failed: {e}")


def _detail_text(err):
    """The store's own words for a refusal, if it gave any."""
    detail = _safe_detail(err)
    if isinstance(detail, dict):
        detail = detail.get("message") or detail.get("conflict") or detail
    return detail if isinstance(detail, str) else (json.dumps(detail) if detail else "")


def _read_exactly(fp, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = fp.read(n - len(buf))
        if not chunk:
            raise _BatchRefused("the stream ended mid-frame")
        buf += chunk
    return bytes(buf)


def _blobs2_stream(base, token, digests):
    """Yield (digest, bytes) for a batch, streaming the framed reply so only one
    blob is in memory at a time. Raises _MissingDigests (fatal) or _BatchRefused
    (recoverable)."""
    body = json.dumps({"digests": digests}).encode()
    try:
        resp = _request(base + "/blobs2", token, data=body)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise KuraError("the store rejected the key (401) — check KURA_KEY")
        if e.code == 404:
            detail = _safe_detail(e)
            if isinstance(detail, dict) and "missing" in detail:
                raise _MissingDigests(detail["missing"])
            raise KuraError("this store has no POST /blobs2 — it needs a kura new enough to serve it")
        if e.code == 410:
            raise KuraError("this store still serves the retired POST /blobs, not /blobs2 — update the store")
        raise _BatchRefused(f"/blobs2 -> HTTP {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise _BatchRefused(f"/blobs2 connection failed: {e}")
    with resp:
        while True:
            first = resp.read(1)
            if not first:
                return  # clean end of stream at a frame boundary
            (dlen,) = struct.unpack(">I", first + _read_exactly(resp, 3))
            digest = _read_exactly(resp, dlen).decode("ascii")
            (blen,) = struct.unpack(">Q", _read_exactly(resp, 8))
            yield digest, _read_exactly(resp, blen)


def _safe_detail(err):
    try:
        return json.loads(err.read()).get("detail")
    except Exception:
        return None


def _blob_single(base, token, digest):
    last = None
    for i in range(len(BACKOFF) + 1):
        try:
            with _request(base + f"/blob/{digest}", token) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise _MissingDigests([digest])
            if e.code == 401:
                raise KuraError("the store rejected the key (401) — check KURA_KEY")
            last = KuraError(f"GET /blob/{digest} -> HTTP {e.code}")
        except (urllib.error.URLError, OSError) as e:
            last = KuraError(f"GET /blob/{digest} failed: {e}")
        if i < len(BACKOFF):
            time.sleep(BACKOFF[i])
    raise last


def _deliver(base, token, digests, on_blob):
    """Deliver every digest to on_blob(digest, bytes). Try the whole batch over
    /blobs2; if it is refused, retry once on a transient hiccup, then split, and
    finally fall back to one GET /blob per digest — so a refused batch never
    fails the package whole."""
    remaining = list(digests)
    if not remaining:
        return
    got = set()
    try:
        for digest, data in _blobs2_stream(base, token, remaining):
            on_blob(digest, data)
            got.add(digest)
        return
    except _BatchRefused:
        rest = [d for d in remaining if d not in got]
        if not rest:
            return
        if len(rest) == 1:
            on_blob(rest[0], _blob_single(base, token, rest[0]))
            return
        if got:
            # partial progress: the remainder is a smaller batch, try it whole
            _deliver(base, token, rest, on_blob)
            return
        # no progress on a multi-digest batch: the reply is likely too big, halve it
        mid = len(rest) // 2
        _deliver(base, token, rest[:mid], on_blob)
        _deliver(base, token, rest[mid:], on_blob)


# --- tags ------------------------------------------------------------------
#
# A tag is a named place on the store: a package published on it is read only
# by a build on the same tag, and through the tag every other package still
# comes from main. An older store ignores `tag` wherever it appears — it would
# serve main to a tag build, and land a tag publish on main — so every command
# that is given a tag first makes the store confirm it knows the tag.

def _tag_of(tag):
    """The tag asked for (else $KURA_TAG), or None for main. `main` is not a
    tag but the absence of one, and a build ARG that nobody passed is empty."""
    if tag is None:
        tag = os.environ.get("KURA_TAG")
    tag = (tag or "").strip()
    return None if tag in ("", "main") else tag


def _too_old(tag):
    return KuraError(
        f"this store does not know tags — it would ignore tag {tag!r} and use main. "
        f"It needs a kura new enough to serve tags.")


def _tag_base(base, token, tag):
    """The tag's own base, from a store that confirms it knows the tag."""
    body = _get_json(base, f"/base?tag={_quote(tag)}", token)
    if body.get("tag") != tag:
        raise _too_old(tag)
    return body["base"]


def _endpoint(url, key):
    base = (url or os.environ.get("KURA_URL") or DEFAULT_URL).rstrip("/")
    token = key or os.environ.get("KURA_KEY")
    if not token:
        raise KuraError("no key: set KURA_KEY or pass --key")
    return base, token


# --- fetch -----------------------------------------------------------------

def _on_disk_matches(path: Path, digest: str) -> bool:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() == digest
    except OSError:
        return False


# a consumer's own note beside the tree; never a package's file, never pruned
PROVENANCE = ".provenance.json"

# resolve's record beside the tree: the roots requested, the packages the
# closure covered, the base the store read them at, each package's public root,
# and where each package came from (`published`) — a build's note of which
# world it drew from. Ours, never a package's
# file, never pruned.
CLOSURE = ".closure.json"

# sync also writes these from the closure: the vite/vitest alias map (inside the
# tree) and tsc's paths (at the repo root, where tsconfig.json extends it). Both
# generated, both gitignored by the consumer — no alias or root logic is kept by
# hand in a consumer any more.
ALIASES = ".aliases.json"
TSCONFIG_PATHS = "tsconfig.paths.json"


def fetch(package, dest, url=None, key=None, strip=True, dry_run=False, prune=False, tag=None):
    """Materialise `package` from the store into `dest`.

    Returns a summary dict: files, written, skipped, fetched_blobs, fetched_bytes
    (and pruned, when asked). By default the leading `<package>/` prefix is
    stripped so `dest` holds the package's tree directly; pass strip=False to
    keep it. With prune=True, files under `dest` that the package no longer
    lists are removed afterwards (a consumer's `.provenance.json` is left), so
    a re-sync moves only what changed and leaves nothing stale behind. With a
    tag (default $KURA_TAG), the package is read as a build on that tag sees it.
    """
    base, token = _endpoint(url, key)
    dest = Path(dest)
    tag = _tag_of(tag)
    query = f"/manifest?package={_quote(package)}"
    if tag:
        # a manifest cannot say whether the tag was honoured, so ask first
        _tag_base(base, token, tag)
        query += f"&tag={_quote(tag)}"

    manifest = _get_json(base, query, token)
    if not manifest:
        raise KuraError(f"the store has no package named {package!r} (empty manifest)")

    def out_path(display: str) -> Path:
        rel = display
        if strip and (display == package or display.startswith(package + "/")):
            rel = display[len(package) + 1:]
        return dest / rel

    # digest -> the paths that carry it (content-addressed: one blob, many paths)
    by_digest: dict[str, list[Path]] = {}
    for display, digest in manifest.items():
        by_digest.setdefault(digest, []).append(out_path(display))

    needed, skipped = [], 0
    for digest, paths in by_digest.items():
        if all(_on_disk_matches(p, digest) for p in paths):
            skipped += len(paths)
        else:
            needed.append(digest)

    stale = _stale(dest, {p for ps in by_digest.values() for p in ps}) if prune else []

    if dry_run:
        return {"files": len(manifest), "written": 0, "skipped": skipped,
                "fetched_blobs": 0, "fetched_bytes": 0, "would_fetch_blobs": len(needed),
                "would_prune": len(stale), "tag": tag}

    summary = {"files": len(manifest), "written": 0, "skipped": skipped,
               "fetched_blobs": 0, "fetched_bytes": 0, "pruned": 0, "tag": tag}

    def on_blob(digest, data):
        if hashlib.sha256(data).hexdigest() != digest:
            raise KuraError(f"the store returned the wrong bytes for {digest} (digest mismatch)")
        summary["fetched_blobs"] += 1
        summary["fetched_bytes"] += len(data)
        for p in by_digest[digest]:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            summary["written"] += 1

    _deliver(base, token, needed, on_blob)
    for p in stale:
        p.unlink()
        summary["pruned"] += 1
        # and the directories it leaves empty, up to dest
        d = p.parent
        while d != dest and d.is_dir() and not any(d.iterdir()):
            d.rmdir()
            d = d.parent
    return summary


def _stale(dest: Path, keep: set) -> list:
    """Files under `dest` that are not the package's, in a stable order."""
    if not dest.is_dir():
        return []
    return sorted(p for p in dest.rglob("*")
                  if p.is_file() and p not in keep and p.name != PROVENANCE)


def _quote(s: str) -> str:
    from urllib.parse import quote
    return quote(s, safe="")


# --- resolve: a whole dependency closure in one call -----------------------
#
# `fetch` materialises one package. A consumer needs the package AND everything
# it depends on, and it used to get that by listing the closure itself and
# fetching a package at a time. The store can walk its own dependency graph, so
# `resolve` asks for the closure of some roots (GET /closure) and fetches every
# blob it names in one pass — the same content-addressed /blobs2 as fetch, so
# blobs shared across packages move once. The tree lands at dest/<package>/...,
# the layout a consumer aliases `@<package>` against.

def resolve(dest, roots, url=None, key=None, dry_run=False, prune=True, tag=None):
    """Materialise the transitive closure of `roots` into `dest`.

    Returns a summary dict: packages, base, files, written, skipped,
    fetched_blobs, fetched_bytes, and pruned/would_* as for `fetch`. Writes
    `.closure.json` beside the tree — the roots, the packages resolved and the
    base the store read them at — so a build records exactly which world it
    drew from. With prune (the default) files the closure no longer names are
    removed, so a re-resolve leaves nothing stale from a dependency that left.

    With a tag (default $KURA_TAG), each package the tag carries comes from the
    tag and every other from main; the summary and `.closure.json` then also
    record the tag, the packages it supplied (`tagged`) and its `tag_base`."""
    base_url, token = _endpoint(url, key)
    dest = Path(dest)
    tag = _tag_of(tag)

    q = ",".join(_quote(r) for r in roots)
    body = _get_json(base_url, f"/closure?roots={q}" + (f"&tag={_quote(tag)}" if tag else ""), token)
    if tag and body.get("tag") != tag:
        raise _too_old(tag)
    manifest: dict[str, str] = body.get("manifest", {})
    packages: list[str] = body.get("packages", [])
    store_base = body.get("base")
    roots_map: dict[str, str] = body.get("roots", {})  # package -> its public root
    # package -> its latest landing as the store reports it: who, when, why, and
    # the repo, branch and commit it was published from (a store from before
    # sources reports none)
    published: dict[str, dict] = body.get("published", {})

    # A root that does not resolve — a typo, a package not yet published, or the
    # wrong store — comes back simply absent from the closure, not as an error.
    # With prune on (the default) an empty or partial closure would then delete
    # everything under `dest`. Refuse it: a closure missing any requested root is
    # degenerate, never something to materialise over a good tree. (fetch guards
    # the same way with its empty-manifest check.)
    missing = [r for r in roots if r not in packages]
    if missing:
        raise KuraError(
            f"closure did not resolve root(s): {', '.join(missing)} — refusing to touch "
            f"{dest} (a typo, an unpublished package, or the wrong KURA_URL would "
            f"otherwise prune the whole tree)")

    by_digest: dict[str, list[Path]] = {}
    for display, digest in manifest.items():
        by_digest.setdefault(digest, []).append(dest / display)

    needed, skipped = [], 0
    for digest, paths in by_digest.items():
        if all(_on_disk_matches(p, digest) for p in paths):
            skipped += len(paths)
        else:
            needed.append(digest)

    keep = {p for ps in by_digest.values() for p in ps}
    keep.add(dest / CLOSURE)  # the lockfile is ours, never stale
    stale = _stale(dest, keep) if prune else []

    # which world this is: main alone, or main seen through a tag
    world = {"tag": tag, "tagged": body.get("tagged", []), "tag_base": body.get("tag_base")} if tag else {}

    if dry_run:
        return {"packages": packages, "base": store_base, "roots": roots_map, "files": len(manifest),
                "written": 0, "skipped": skipped, "fetched_blobs": 0, "fetched_bytes": 0,
                "would_fetch_blobs": len(needed), "would_prune": len(stale), "published": published, **world}

    summary = {"packages": packages, "base": store_base, "roots": roots_map, "files": len(manifest),
               "written": 0, "skipped": skipped, "fetched_blobs": 0, "fetched_bytes": 0, "pruned": 0,
               "published": published, **world}

    def on_blob(digest, data):
        if hashlib.sha256(data).hexdigest() != digest:
            raise KuraError(f"the store returned the wrong bytes for {digest} (digest mismatch)")
        summary["fetched_blobs"] += 1
        summary["fetched_bytes"] += len(data)
        for p in by_digest[digest]:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            summary["written"] += 1

    _deliver(base_url, token, needed, on_blob)
    for p in stale:
        p.unlink()
        summary["pruned"] += 1
        d = p.parent
        while d != dest and d.is_dir() and not any(d.iterdir()):
            d.rmdir()
            d = d.parent
    dest.mkdir(parents=True, exist_ok=True)
    (dest / CLOSURE).write_text(
        json.dumps({"requested": list(roots), "packages": packages, "base": store_base,
                    "roots": roots_map, "published": published, **world}, indent=2, sort_keys=True) + "\n")
    return summary


# --- sync: resolve, and write the consumer's generated harness ---------------
#
# resolve materialises the closure; a TypeScript consumer then needs two files
# generated from it — the vite/vitest alias map and tsconfig's paths — both of
# which used to be written by hand in every repo, each carrying its own copy of
# the roots map. sync writes them from the store's own data (the roots come down
# in the closure), so a consumer keeps no alias or root logic of its own: its
# direct deps in packages.json, and a one-line read of the generated files.

def _harness(dest: Path, packages, roots_map):
    """The alias map and tsconfig paths for a materialised closure. Values are
    relative to the repo root (dest's parent), so vite resolves each against its
    config dir and tsc against baseUrl '.'."""
    rel = dest.name  # 'ext' by convention; dest is <repo>/ext
    exts = ("index.ts", "index.tsx", "index.js", "index.jsx", "index.mjs", "index.cjs")
    aliases: dict[str, str] = {}
    ts_paths: dict[str, list[str]] = {}
    for name in packages:
        root = roots_map.get(name, "src")
        base = f"{rel}/{name}/{root}"
        aliases[f"@{name}"] = base
        ts_paths[f"@{name}/*"] = [f"{base}/*"]
        # a bare @<name> resolves to the package index; match the extensions vite
        # resolves a directory index across, so tsc and vite agree
        for ext in exts:
            if (dest / name / root / ext).exists():
                ts_paths[f"@{name}"] = [f"{base}/{ext}"]
                break
    return aliases, ts_paths


def sync(dest, roots, url=None, key=None, dry_run=False, prune=True, tag=None):
    """Resolve the closure of `roots` into `dest`, then write the consumer's
    generated harness beside it: `<dest>/.aliases.json` (the vite/vitest alias
    map) and `<repo>/tsconfig.paths.json` (tsc's paths), both from the store's
    closure data. Returns resolve's summary plus `aliased` (the alias count).
    A tag (default $KURA_TAG) is read through as for `resolve`."""
    res = resolve(dest, roots, url=url, key=key, dry_run=dry_run, prune=prune, tag=tag)
    if dry_run:
        return res
    dest = Path(dest)
    aliases, ts_paths = _harness(dest, res["packages"], res.get("roots", {}))
    (dest / ALIASES).write_text(json.dumps(aliases, indent=2, sort_keys=True) + "\n")
    (dest.parent / TSCONFIG_PATHS).write_text(
        json.dumps({"compilerOptions": {"baseUrl": ".", "paths": ts_paths}}, indent=2, sort_keys=True) + "\n")
    res["aliased"] = len(aliases)
    return res


# --- publish: hand a package's whole tree to the store ----------------------
#
# The store's publish verb sets a package's tree to exactly the files it is
# given, burying whatever they no longer include, in one changeset. `publish`
# gathers those files from a directory — all of it, or the paths named under it
# — and hands them over against the base it has just read, re-reading and
# retrying if another publish moved that base first. With a tag (default
# $KURA_TAG) the tree lands on the tag and main does not move.

# never package content, whatever directory is published
SKIP_DIRS = {".git", "node_modules", "__pycache__"}


def _collect(src: Path, paths) -> dict:
    """relpath (under src, posix) -> bytes for every file the publish covers."""
    if not src.is_dir():
        raise KuraError(f"{src} is not a directory")
    root = src.resolve()
    files: dict[str, bytes] = {}
    for named in paths or ["."]:
        target = (src / named).resolve()
        if target != root and root not in target.parents:
            raise KuraError(f"{named} is not under {src}")
        if not target.exists():
            raise KuraError(f"{named} does not exist under {src}")
        if target.is_file():
            files[target.relative_to(root).as_posix()] = target.read_bytes()
            continue
        for p in sorted(target.rglob("*")):
            if p.is_file() and not SKIP_DIRS & set(p.relative_to(target).parts):
                files[p.relative_to(root).as_posix()] = p.read_bytes()
    return dict(sorted(files.items()))


# --- source: where a published tree came from -------------------------------
#
# The store cannot reach a repo, so a publish says where its tree came from and
# the store keeps that word. It is read from git in the published directory:
# the repo, the branch and the commit. The repo is NAMED (owner/name), never
# addressed — an origin URL may carry a token, and nothing of it but the name
# leaves the machine.

def _git(cwd, *args):
    """git's answer in `cwd`, or None when git is absent or says no."""
    try:
        r = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    out = r.stdout.strip()
    return out if r.returncode == 0 and out else None


def _repo_name(remote):
    """owner/name from any form of origin: https (with or without a token),
    ssh, scp-like (git@host:owner/name) or a proxy's path — the last two
    segments of its path, without `.git`. Never the host, never credentials."""
    path = remote.strip().rstrip("/")
    if "://" in path:
        path = path.split("://", 1)[1].split("/", 1)[1] if "/" in path.split("://", 1)[1] else ""
    elif ":" in path and not path.startswith("/"):
        path = path.split(":", 1)[1]  # scp-like: [user@]host:owner/name
    if path.endswith(".git"):
        path = path[:-4]
    parts = [p for p in path.split("/") if p]
    return "/".join(parts[-2:]) if parts else None


def git_source(src):
    """{repo, branch, commit} for the git checkout holding `src` — whichever
    git can say — or None when `src` is not in git. A detached HEAD has no
    branch; a checkout with no origin has no repo."""
    commit = _git(src, "rev-parse", "HEAD")
    if commit is None:
        return None
    source = {"commit": commit}
    branch = _git(src, "symbolic-ref", "--short", "-q", "HEAD")
    if branch:
        source["branch"] = branch
    remote = _git(src, "remote", "get-url", "origin")
    repo = _repo_name(remote) if remote else None
    if repo:
        source["repo"] = repo
    return source


def _source_text(source):
    """A source in a few words: repo@branch and the short commit."""
    if not source:
        return ""
    where = "@".join(x for x in (source.get("repo"), source.get("branch")) if x)
    commit = (source.get("commit") or "")[:7]
    return " ".join(x for x in (where, commit) if x)


def publish(package, src, paths=None, message=None, tag=None, who=None, url=None, key=None, dry_run=False,
            source=None, read_source=True):
    """Set `package`'s tree in the store to the files under `src` (or under the
    `paths` named within it), keyed relative to `src`.

    The publish says where the tree came from: `source` ({repo, branch,
    commit}), read from git in `src` unless `read_source` is off; whatever
    `source` names itself wins over what git says.

    Returns a summary dict: package, tag (None for main), files, bytes, source,
    and — once sent — seq, written and buried, as the store reports them."""
    base_url, token = _endpoint(url, key)
    tag = _tag_of(tag)
    who = who or os.environ.get("KURA_WHO") or "kura-cli"
    files = _collect(Path(src), paths)
    if not files:
        raise KuraError(f"nothing to publish under {src}")
    said = {k: v for k, v in (source or {}).items() if v}
    source = {**((git_source(src) or {}) if read_source else {}), **said} or None
    summary = {"package": package, "tag": tag, "files": len(files),
               "bytes": sum(len(b) for b in files.values()), "source": source}
    if dry_run:
        summary["paths"] = list(files)
        return summary
    encoded = {rel: base64.b64encode(data).decode() for rel, data in files.items()}
    last = None
    for attempt in range(len(RACE_BACKOFF) + 1):
        if attempt:
            time.sleep(RACE_BACKOFF[attempt - 1])
        # read the base anew each time: a race means another publish moved it
        store_base = _tag_base(base_url, token, tag) if tag else _get_json(base_url, "/base", token)["base"]
        body = {"who": who, "base": store_base, "message": message or "", "files": encoded}
        if tag:
            body["tag"] = tag
        if source:
            body["source"] = source
        try:
            with _request(base_url + f"/packages/{_quote(package)}/publish", token,
                          data=json.dumps(body).encode()) as r:
                res = json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise KuraError(f"the store rejected the key ({e.code}) — check KURA_KEY")
            if e.code == 409 or e.code >= 500:
                last = f"HTTP {e.code}: {_detail_text(e)}"
                continue
            raise KuraError(f"the store refused the publish (HTTP {e.code}): {_detail_text(e)}")
        except (urllib.error.URLError, OSError) as e:
            # a restated tree records nothing, so sending it again is safe
            last = str(e)
            continue
        summary.update(seq=res.get("seq"), written=res.get("written"), buried=res.get("buried", []))
        return summary
    raise KuraError(f"could not publish {package} after {len(RACE_BACKOFF) + 1} attempts ({last})")


# --- CLI -------------------------------------------------------------------

def _add_common(p):
    p.add_argument("--url", help="store base URL (default $KURA_URL or the staging store)")
    p.add_argument("--key", help="bearer token (default $KURA_KEY)")
    p.add_argument("--tag", help="work on this tag (default $KURA_TAG); 'main' or empty means main")
    p.add_argument("--dry-run", action="store_true", help="report what would happen, change nothing")
    p.add_argument("--quiet", action="store_true", help="print nothing on success")


def _on(tag):
    return f"on tag {tag}" if tag else "on main"


def _drawn(res):
    """Where a build on a tag drew its packages from, in a few words."""
    if not res.get("tag"):
        return ""
    tagged = res.get("tagged") or []
    if not tagged:
        return f" on tag {res['tag']} (nothing on the tag: all from main)"
    return f" on tag {res['tag']} (from the tag: {', '.join(tagged)}; the rest from main)"


def main(argv=None):
    parser = argparse.ArgumentParser(prog="kura", description="fetch and publish packages in a kura store")
    sub = parser.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="materialise a package into a directory")
    f.add_argument("package")
    f.add_argument("dest")
    _add_common(f)
    f.add_argument("--no-strip", dest="strip", action="store_false",
                   help="keep the leading <package>/ prefix on written paths")
    f.add_argument("--prune", action="store_true",
                   help="afterwards, remove files under <dest> the package no longer lists")
    # Accepted so that it can be refused, at the source, in the team's words:
    # pinning is not helpful in active development, so nobody pins.
    f.add_argument("--pin", metavar="PIN", help=argparse.SUPPRESS)

    r = sub.add_parser("resolve", help="materialise root packages and their whole dependency closure")
    r.add_argument("dest")
    r.add_argument("roots", nargs="+", help="root packages; their transitive closure is fetched")
    _add_common(r)
    r.add_argument("--no-prune", dest="prune", action="store_false",
                   help="keep files the closure no longer names (default: prune them)")

    s = sub.add_parser("sync", help="resolve a closure and write the consumer's generated harness")
    s.add_argument("dest")
    s.add_argument("roots", nargs="+", help="root packages; their transitive closure is fetched")
    _add_common(s)
    s.add_argument("--no-prune", dest="prune", action="store_false",
                   help="keep files the closure no longer names (default: prune them)")

    pb = sub.add_parser("publish", help="set a package's tree to the files under a directory")
    pb.add_argument("package")
    pb.add_argument("src", help="the directory the package's paths are relative to")
    pb.add_argument("paths", nargs="*", help="files or directories under <src> to publish (default: all of it)")
    pb.add_argument("-m", "--message", required=True, help="why this publish was made (12-256 characters)")
    pb.add_argument("--who", help="who the publish is signed by (default $KURA_WHO or kura-cli)")
    pb.add_argument("--source-repo", help="the repo the tree came from, owner/name (default: read from git)")
    pb.add_argument("--source-branch", help="the branch it came from (default: read from git)")
    pb.add_argument("--source-commit", help="the commit it came from (default: read from git)")
    pb.add_argument("--no-source", dest="read_source", action="store_false",
                    help="do not read git for where the tree came from")
    _add_common(pb)

    args = parser.parse_args(argv)

    if getattr(args, "pin", None) is not None:
        print(f"kura: --pin {args.pin}: kura-cli does not support pinning, because it is not "
              "helpful in active development; this is a team level message, do not pin packages.",
              file=sys.stderr)
        return 2

    try:
        if args.cmd == "publish":
            said = {"repo": args.source_repo, "branch": args.source_branch, "commit": args.source_commit}
            res = publish(args.package, args.src, args.paths, message=args.message, tag=args.tag,
                          who=args.who, url=args.url, key=args.key, dry_run=args.dry_run,
                          source=said, read_source=args.read_source)
        elif args.cmd == "sync":
            res = sync(args.dest, args.roots, url=args.url, key=args.key,
                       dry_run=args.dry_run, prune=args.prune, tag=args.tag)
        elif args.cmd == "resolve":
            res = resolve(args.dest, args.roots, url=args.url, key=args.key,
                          dry_run=args.dry_run, prune=args.prune, tag=args.tag)
        else:
            res = fetch(args.package, args.dest, url=args.url, key=args.key,
                        strip=args.strip, dry_run=args.dry_run, prune=args.prune, tag=args.tag)
    except KuraError as e:
        print(f"kura: {e}", file=sys.stderr)
        return 1

    if args.quiet:
        return 0
    if args.cmd == "publish":
        mb = res["bytes"] / 1e6
        came = f" from {_source_text(res['source'])}" if res.get("source") else ""
        if args.dry_run:
            print(f"kura: would publish {args.package} {_on(res['tag'])}{came}: {res['files']} file(s), "
                  f"{mb:.1f} MB — nothing sent")
        else:
            buried = res.get("buried") or []
            print(f"kura: published {args.package} {_on(res['tag'])}{came}: {res['files']} file(s), "
                  f"{res.get('written')} written, {len(buried)} buried ({mb:.1f} MB)"
                  + (f"; buried {', '.join(buried)}" if buried else ""))
    elif args.cmd == "sync":
        n = len(res["packages"])
        if args.dry_run:
            print(f"kura: closure of {'+'.join(args.roots)} @base {res['base']}{_drawn(res)}: {n} package(s); "
                  f"would fetch {res['would_fetch_blobs']}")
        else:
            mb = res["fetched_bytes"] / 1e6
            print(f"kura: synced {n} package(s) -> {args.dest} @base {res['base']}{_drawn(res)}; "
                  f"wrote {res['written']}, pruned {res['pruned']} ({mb:.1f} MB); "
                  f"harness: {res['aliased']} aliases + tsconfig paths")
    elif args.cmd == "resolve":
        n = len(res["packages"])
        if args.dry_run:
            print(f"kura: closure of {'+'.join(args.roots)} @base {res['base']}{_drawn(res)}: {n} package(s), "
                  f"{res['files']} file(s); would fetch {res['would_fetch_blobs']}, would prune {res['would_prune']}")
        else:
            mb = res["fetched_bytes"] / 1e6
            print(f"kura: resolved {n} package(s) -> {args.dest} @base {res['base']}{_drawn(res)}; "
                  f"wrote {res['written']}, skipped {res['skipped']}, pruned {res['pruned']} "
                  f"({res['fetched_blobs']} blob(s), {mb:.1f} MB)")
    elif args.dry_run:
        prune = f", would prune {res['would_prune']}" if args.prune else ""
        on = f" {_on(res['tag'])}" if res.get("tag") else ""
        print(f"kura: {args.package}{on}: {res['files']} file(s); "
              f"{res['skipped']} already present, would fetch {res['would_fetch_blobs']} blob(s){prune}")
    else:
        mb = res["fetched_bytes"] / 1e6
        prune = f", pruned {res['pruned']}" if args.prune else ""
        on = f" {_on(res['tag'])}" if res.get("tag") else ""
        print(f"kura: {args.package}{on}: {res['files']} file(s) -> {args.dest}; "
              f"wrote {res['written']}, skipped {res['skipped']}{prune} "
              f"({res['fetched_blobs']} blob(s), {mb:.1f} MB fetched)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
