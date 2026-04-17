# proto-indexer

Fetches GitHub repositories and produces per-repo SQLite indexes:

- `<repo>.source.sqlite` — source files + FTS5
- `<repo>.packages.sqlite` — one row per proto package, with per-package `FileDescriptorSet` blob
- `<repo>.symbols.sqlite` — one row per top-level message / enum / service / method

Packages and symbols are split so remote clients can fetch the (small)
symbols DB without pulling the (large) descriptor blobs unless needed.
The two DBs are correlated by `proto_package` text, not an FK.

Depends on [`proto-repo`](https://github.com/accretional/proto-repo) for GitHub enumeration (`scan`) and
cloning (`gitfetch`). Everything downstream of "I have a checked-out repo
directory" lives here.

## Layout

| Path | Role |
|---|---|
| `cmd/indexer/` | CLI entrypoint (`--org` or `--repo`) |
| `protocompile/` | Invokes `protoc` to produce a `FileDescriptorSet` |
| `index/source/` | Walks repo, writes `files` rows |
| `index/protos/` | Walks FDS, writes `packages` + `symbols` rows |
| `schema/` | Embedded DDL (`source.sql`, `protos.sql`) |

## Build / test

```
go build ./cmd/indexer
go test ./...
```

## Run

```
go run ./cmd/indexer --repo accretional/proto-merge --token "$GITHUB_TOKEN"
go run ./cmd/indexer --org  accretional --token "$GITHUB_TOKEN"
```
