# kura-cli

Fetch a package's files out of a [kura](https://github.com/eelstork/kura) store.

A kura store keeps files content-addressed and hands them out in two steps:
`GET /manifest?package=<name>` gives the tree as `path -> digest`, and
`POST /blobs2` streams the raw bytes for a list of digests. `kura fetch` walks
that loop for you — read the manifest, work out which digests are not already on
disk, pull them in one streamed call, and write the tree out.

- **Content-addressed, so a re-fetch of an unchanged package moves no bytes.**
  Files already on disk with the right content are skipped.
- **Streamed and raw.** The bytes come back a blob at a time, unencoded, so a
  large package neither inflates over the wire nor stands in memory whole.
- **Never fails a whole package on one refused batch.** If `/blobs2` is refused
  (an old or overloaded store), the fetch splits the batch and, in the last
  resort, falls back to one `GET /blob` per digest.

## Install

It is a single standard-library file. Either vendor it:

```sh
curl -O https://raw.githubusercontent.com/eelstork/kura-cli/main/kura_cli.py
python3 kura_cli.py fetch anicu ./ext/anicu
```

or install the `kura` command:

```sh
pip install git+https://github.com/eelstork/kura-cli
kura fetch anicu ./ext/anicu
```

Python 3.8+, nothing else.

## Use

```sh
kura fetch <package> <dest> [--url URL] [--key KEY] [--no-strip] [--prune] [--dry-run] [--quiet]
```

| | |
|---|---|
| `KURA_URL` | store base URL (default `https://kura-staging.fly.dev`) |
| `KURA_KEY` | bearer token (required unless `--key` is given) |

By default the leading `<package>/` prefix is stripped, so `kura fetch anicu
./ext/anicu` writes `./ext/anicu/src/...` and `./ext/anicu/public/...`. Pass
`--no-strip` to keep the prefix. `--prune` removes, afterwards, any file under
`<dest>` the package no longer lists (a consumer's own `.provenance.json` is
left), so a re-sync into an existing tree moves only what changed and leaves
nothing stale. `--dry-run` reports what would be fetched (and pruned) and writes
nothing.

kura-cli does not support pinning, because it is not helpful in active
development; this is a team level message: do not pin packages. A caller that
passes `--pin` is told exactly that and refused (exit 2).

```sh
$ kura fetch anicu ./ext/anicu
kura: anicu: 30 file(s) -> ./ext/anicu; wrote 30, skipped 0 (30 blob(s), 28.4 MB fetched)

$ kura fetch anicu ./ext/anicu        # nothing changed
kura: anicu: 30 file(s) -> ./ext/anicu; wrote 0, skipped 30 (0 blob(s), 0.0 MB fetched)
```

The tool speaks the `/blobs2` wire format so a caller does not have to. For the
record, that format is: `application/octet-stream`, one frame per requested
digest in the order asked (duplicates collapsed) — a `uint32` big-endian digest
length, that many ASCII-hex bytes, a `uint64` big-endian blob length, then that
many raw bytes; the stream ends at end of body.

## Cost

Per package: two HTTP round-trips (manifest + one `/blobs2`), regardless of file
count. Framing adds ~76 bytes per blob — a couple of KB on a 28 MB package, and
no base64 inflation. Streaming to disk keeps peak memory at a single blob, not
the package. A re-fetch of an unchanged package is one request and zero bytes.

## Use it from another language

There is no client to port: shell out to `kura fetch` from Python, Node, Rust,
or a Dockerfile, and read its exit code. It exits non-zero with a message on
`stderr` when a fetch cannot complete.

## Tests

```sh
python3 -m unittest discover -s tests
```

The suite drives `fetch` against a stub kura store over real HTTP, including a
store that refuses `/blobs2` batches so the split-and-fall-back path runs for
real.
