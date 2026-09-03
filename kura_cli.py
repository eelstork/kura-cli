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
    kura fetch <package> <dest> [--url URL] [--key KEY] [--no-strip] [--dry-run]

    KURA_URL   store base URL   (default https://kura-staging.fly.dev)
    KURA_KEY   bearer token     (required unless --key is given)

Speaks only the Python standard library, so a session can vendor this one file
and run it with nothing to install.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_URL = "https://kura-staging.fly.dev"
BACKOFF = (2, 4, 8)  # seconds between retries of a transient network failure
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
        raise KuraError(f"GET {path} -> HTTP {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise KuraError(f"GET {path} failed: {e}")


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


# --- fetch -----------------------------------------------------------------

def _on_disk_matches(path: Path, digest: str) -> bool:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() == digest
    except OSError:
        return False


def fetch(package, dest, url=None, key=None, strip=True, dry_run=False):
    """Materialise `package` from the store into `dest`.

    Returns a summary dict: files, written, skipped, fetched_blobs, fetched_bytes.
    By default the leading `<package>/` prefix is stripped so `dest` holds the
    package's tree directly; pass strip=False to keep it.
    """
    base = (url or os.environ.get("KURA_URL") or DEFAULT_URL).rstrip("/")
    token = key or os.environ.get("KURA_KEY")
    if not token:
        raise KuraError("no key: set KURA_KEY or pass --key")
    dest = Path(dest)

    manifest = _get_json(base, f"/manifest?package={_quote(package)}", token)
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

    if dry_run:
        return {"files": len(manifest), "written": 0, "skipped": skipped,
                "fetched_blobs": 0, "fetched_bytes": 0, "would_fetch_blobs": len(needed)}

    summary = {"files": len(manifest), "written": 0, "skipped": skipped,
               "fetched_blobs": 0, "fetched_bytes": 0}

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
    return summary


def _quote(s: str) -> str:
    from urllib.parse import quote
    return quote(s, safe="")


# --- CLI -------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(prog="kura", description="fetch a package out of a kura store")
    sub = parser.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="materialise a package into a directory")
    f.add_argument("package")
    f.add_argument("dest")
    f.add_argument("--url", help="store base URL (default $KURA_URL or the staging store)")
    f.add_argument("--key", help="bearer token (default $KURA_KEY)")
    f.add_argument("--no-strip", dest="strip", action="store_false",
                   help="keep the leading <package>/ prefix on written paths")
    f.add_argument("--dry-run", action="store_true", help="report what would be fetched, write nothing")
    f.add_argument("--quiet", action="store_true", help="print nothing on success")
    args = parser.parse_args(argv)

    try:
        res = fetch(args.package, args.dest, url=args.url, key=args.key,
                    strip=args.strip, dry_run=args.dry_run)
    except KuraError as e:
        print(f"kura: {e}", file=sys.stderr)
        return 1

    if not args.quiet:
        if args.dry_run:
            print(f"kura: {args.package}: {res['files']} file(s); "
                  f"{res['skipped']} already present, would fetch {res['would_fetch_blobs']} blob(s)")
        else:
            mb = res["fetched_bytes"] / 1e6
            print(f"kura: {args.package}: {res['files']} file(s) -> {args.dest}; "
                  f"wrote {res['written']}, skipped {res['skipped']} "
                  f"({res['fetched_blobs']} blob(s), {mb:.1f} MB fetched)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
