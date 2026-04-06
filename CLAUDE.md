# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## OpenWolf

@.wolf/OPENWOLF.md

This project uses OpenWolf for context management. Read and follow .wolf/OPENWOLF.md every session. Check .wolf/cerebrum.md before generating code. Check .wolf/anatomy.md before reading files.

## Project Overview

Snowflake Emulator — a lightweight Snowflake-compatible SQL interface backed by DuckDB, for local dev and CI testing. Written in Go 1.24+, uses chi router, DuckDB via CGO. No auth; purely a dev/test tool.

## Build & Test Commands

```bash
# Build
go build ./...                           # or: CGO_ENABLED=1 go build -o snowflake-emulator ./cmd/server

# Run server (in-memory, port 8080)
make run                                 # or: go run cmd/server/main.go

# Tests
make test                                # unit tests only (pkg/...)
make test-integration                    # integration tests (tests/integration/...)
make test-e2e                            # e2e tests (tests/e2e/...)
make test-all                            # all tests
go test -v -race ./pkg/query/...         # single package
go test -v -race -run TestTranslator_IFF ./pkg/query/...  # single test

# Lint (golangci-lint v2, config in .golangci.yml)
make lint                                # or: golangci-lint run --timeout=5m

# Format
make fmt
```

## Architecture

Two API surfaces served from a single HTTP server (`cmd/server/main.go`):

1. **gosnowflake protocol** (`/session/*`, `/queries/*`) — implements the wire protocol used by the `gosnowflake` Go driver. Handlers in `server/handlers/session.go` and `server/handlers/query.go`.
2. **REST API v2** (`/api/v2/*`) — Snowflake's HTTP REST API for statements, databases, schemas, tables, warehouses. Handler in `server/handlers/rest_api_v2.go`.

### Core packages (`pkg/`)

- **`query/`** — Central package. `Executor` runs SQL against DuckDB. `Translator` rewrites Snowflake SQL to DuckDB SQL via AST manipulation (using vitess-sqlparser) with string-based fallback for unparseable SQL. `CopyProcessor` handles `COPY INTO`, `MergeProcessor` handles `MERGE INTO`, `StreamProcessor` handles Streams/CDC. `Classifier` categorizes SQL statements (DDL/DML/query/SHOW/DESCRIBE/streams). `StatementManager` tracks async statement state for REST API v2. `ddl_parser.go` has regex parsers for CREATE/DROP DATABASE/SCHEMA/STREAM, USE, SHOW, DESCRIBE.
- **`metadata/`** — `Repository` manages Snowflake metadata (databases, schemas, tables, stages, streams, file formats) in DuckDB tables prefixed with `_metadata_`. Includes both legacy methods and `*Catalog()` methods for ATTACH-based database management.
- **`connection/`** — `Manager` wraps `*sql.DB` with a mutex for thread-safe DuckDB access (DuckDB requires serialized writes).
- **`session/`** — `Manager` + `Store` for session lifecycle. Sessions are in-memory with configurable expiration.
- **`stage/`** — `Manager` for internal stage file storage (used by `COPY INTO`). Files stored on local filesystem.
- **`warehouse/`** — `Manager` for virtual warehouse state (in-memory, no actual compute).
- **`types/`** — Snowflake-to-DuckDB type mapping.
- **`config/`** — Configuration constants + `IsCatalogMode()` feature flag.

### Server layer (`server/`)

- **`handlers/`** — HTTP handlers that wire requests to `pkg/` services.
- **`types/`** — Request/response structs for both API surfaces.
- **`apierror/`** — Snowflake-compatible error types with JSON serialization.

### Key data flow

Request → chi router → handler → `query.Executor` → `query.Translator` (Snowflake→DuckDB SQL) → `connection.Manager` (DuckDB) → response formatting.

Table names in DuckDB use the pattern `{database}.{schema}_{table}` (legacy mode) or `"database"."schema"."table"` (catalog mode with `ENABLE_CATALOG_MODE=true`). See `query/table_naming.go`.

### Wiring pattern

`Executor` and processors (`CopyProcessor`, `MergeProcessor`, `StreamProcessor`) have a circular dependency — processors need the executor, executor needs processors. Resolved via post-construction configuration with `executor.Configure(WithCopyProcessor(...), WithMergeProcessor(...), WithStreamProcessor(...))`.

### SQL translation pipeline

Three-tier translation in `query/translator.go`:
1. **AST-based** (vitess-sqlparser) — for standard SELECT/DML with function translations
2. **String fallback** (`applyStringFallbackTranslations`) — regex-based, used when AST parsing fails (three-part names, `::` casts, window functions)
3. **DDL defaults** (`translateDDLDefaults`) — for DEFAULT clauses in CREATE TABLE

### Streams/CDC

Snowflake Streams emulated via shadow changelog tables (`_stream_{name}_changelog`). DML interception in `executeRaw()` writes to changelogs when source tables have active streams. See `design_decisions.md` for architecture details.

## Test Structure

- **Unit tests**: colocated `_test.go` files in `pkg/` packages
- **Integration tests**: `tests/integration/` — full server workflow tests
- **E2E tests**: `tests/e2e/` — tests against running server using gosnowflake driver and REST API
- All tests use race detection (`-race` flag)

## Environment Variables

- `PORT` (default `8080`) — server port
- `DB_PATH` (default `:memory:`) — DuckDB database path
- `STAGE_DIR` (default `./stages`) — internal stage file directory
- `ENABLE_CATALOG_MODE` (default `false`) — when `true`, databases use DuckDB ATTACH catalogs enabling three-part name resolution and CREATE/DROP DATABASE via SQL

## CI

GitHub Actions (`.github/workflows/ci.yaml`): lint → test-all → build + docker. Uses golangci-lint v2.
