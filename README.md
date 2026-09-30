# kura-cli

Fetch and publish a package's files in a [kura](https://github.com/eelstork/kura) store —
on main, or on a tag.

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
kura fetch   <package> <dest> [--tag TAG] [--no-strip] [--prune] [--dry-run] [--quiet]
kura resolve <dest> <root>... [--tag TAG] [--no-prune] [--dry-run] [--quiet]
kura sync    <dest> <root>... [--tag TAG] [--no-prune] [--dry-run] [--quiet]
kura publish <package> <dir> [<path>...] -m MESSAGE [--tag TAG] [--who WHO] [--dry-run] [--quiet]
```

Every command also takes `--url URL` and `--key KEY`.

| | |
|---|---|
| `KURA_URL` | store base URL (default `https://kura-staging.fly.dev`) |
| `KURA_KEY` | bearer token (required unless `--key` is given) |
| `KURA_TAG` | the tag to publish and build on (default: none — main) |
| `KURA_WHO` | who a publish is signed by (default `kura-cli`) |

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

## Publish

`kura publish <package> <dir> [<path>...]` sets the package's tree in the store
to the files under `<dir>` — all of it, or just the files and directories named
under it — keyed by their path relative to `<dir>`. Whatever the package held
that is not in the new tree is buried, in the same changeset. `-m` says why
(the store wants 12–256 characters). `.git`, `node_modules` and `__pycache__`
directories are never published.

```sh
$ kura publish metropolis . src/settle src/supply src/view/bend.ts -m "metropolis: markets on the square"
kura: published metropolis on main: 51 file(s), 3 written, 1 buried (0.4 MB); buried metropolis/src/settle/old.ts
```

It reads the store's base just before sending, and if another publish moved it
first, reads it again and retries. `--dry-run` lists what would be sent and
sends nothing.

## Tags

A tag is where you publish packages that only builds on the same tag will see.
Through a tag, each package the tag carries comes from the tag and every other
package from main; publishing on a tag never moves main. So an agent changing
diarch for metropolis publishes diarch on its tag and builds metropolis on the
same tag — its diarch, everyone else's main — while every other build keeps
reading main.

Pick one tag for a piece of work and set it once:

```sh
export KURA_TAG=fern                      # or pass --tag fern to each command
kura publish diarch . src -m "diarch: taller arches"
kura: published diarch on tag fern: 115 file(s), 2 written, 0 buried (1.2 MB)

kura sync ext metropolis
kura: synced 14 package(s) -> ext @base 1270 on tag fern (from the tag: diarch; the rest from main); ...
```

`--tag main` (or an empty tag) means main, so a session with `KURA_TAG` set can
still publish on main when the work is done. A tag nothing was published on
reads as main. `.closure.json` records `tag`, `tagged` (the packages the tag
supplied) and `tag_base` beside the base, so a build says which world it drew.

Tag names are 1–40 lowercase letters, digits and inner hyphens — a tag also
names a deploy (`metropolis-fern`) — and the store refuses `staging`,
`production` and seven hex digits.

A store too old to know tags would ignore the tag: serve main to a build, and
land a publish on main. kura-cli asks the store to confirm the tag first and
refuses if it cannot.

### Building on a tag

A build that fetches with `kura sync` reads the tag from `KURA_TAG`. For an
image build, declare it in the stage that fetches, so the deployer can pass it
in (kimi does, for a deploy on a tag):

```dockerfile
ARG KURA_TAG
RUN --mount=type=secret,id=KURA_KEY node fetch-packages.mjs && npx vite build
```

and fetch with a kura-cli that knows tags: one from before them ignores
`KURA_TAG` and quietly builds from main. With no tag passed the ARG is empty,
which is main, so the same Dockerfile keeps building main as before.

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
