# proto-indexer

Fetches GitHub repositories and produces per-repo SQLite indexes:

- `<repo>.source.sqlite` — source files (with optional vector embeddings)
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

Index a single repo or an entire org:

```
go run ./cmd/indexer --repo accretional/proto-merge --token "$GITHUB_TOKEN"
go run ./cmd/indexer --org  accretional --token "$GITHUB_TOKEN"
```

Index a local repo:

```
go run ./cmd/indexer --local /path/to/repo
```

Include vector embeddings via the Apple NaturalLanguage framework (macOS):

```
go run ./cmd/indexer --org accretional --embedding-provider apple
```

Store raw file content in the source sqlite (off by default):

```
go run ./cmd/indexer --repo accretional/proto-merge --store-content
```

Generate `index.sqlite` alongside the per-repo files (useful for a query overlay that needs a manifest of available DBs). Use `--site-index-base-path` to set the URL prefix stored in `db_path`:

```
# Alongside a fresh indexing run
go run ./cmd/indexer --org accretional --site-index --site-index-base-path '/repos/'

# Rebuild index.sqlite from an existing output directory
go run ./cmd/indexer --out-dir ./out --site-index --site-index-base-path '/repos/'

# Only include repos that have all three sqlite variants (source + packages + symbols)
go run ./cmd/indexer --out-dir ./out --site-index --site-index-proto-only --site-index-base-path '/repos/'
```
