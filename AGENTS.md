# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

Snowflake Emulator — a lightweight, Snowflake-compatible SQL server backed by DuckDB, for local development and CI testing. Written in Go 1.24+ (chi router, DuckDB via CGO). No auth; purely a dev/test tool. MIT licensed.

It speaks two API surfaces from one HTTP server (`cmd/server/main.go`):

1. **gosnowflake wire protocol** (`/session/*`, `/queries/*`) — used by the official Go driver and Node.js `snowflake-sdk`.
2. **Snowflake REST API v2** (`/api/v2/*`) — statements, databases, schemas, tables, warehouses.

## Build, test, lint

```bash
CGO_ENABLED=1 go build -o snowflake-emulator ./cmd/server   # build
make run                # run server (in-memory, port 8080)
make test               # unit tests (pkg/...)
make test-integration   # tests/integration/...
make test-e2e           # tests/e2e/... (gosnowflake driver + REST API)
make test-all           # everything
make lint               # golangci-lint v2 (.golangci.yml)
make fmt                # format
go test -v -race -run TestTranslator_IFF ./pkg/query/...    # single test
```

All tests run with `-race`. CGO is required (DuckDB).

## Layout

- `cmd/server/` — entry point and wiring.
- `pkg/query/` — central package: `Executor` (runs SQL on DuckDB), `Translator` (Snowflake→DuckDB SQL, AST-based via vitess-sqlparser with regex string fallback), `CopyProcessor` (COPY INTO), `MergeProcessor` (MERGE INTO), `StreamProcessor` (Streams/CDC), `Classifier`, `StatementManager`, `ddl_parser.go`.
- `pkg/metadata/` — Snowflake metadata (databases/schemas/tables/stages/streams) stored in DuckDB tables prefixed `_metadata_`; `*Catalog()` methods for ATTACH-based catalog mode.
- `pkg/connection/` — mutex-wrapped `*sql.DB` (DuckDB needs serialized writes).
- `pkg/session/`, `pkg/stage/`, `pkg/warehouse/`, `pkg/types/`, `pkg/config/` — sessions, internal stage files, virtual warehouses, type mapping, config.
- `server/handlers/` — HTTP handlers; `server/types/` — request/response structs; `server/apierror/` — Snowflake-compatible errors.
- `tests/integration/`, `tests/e2e/` — full-server tests; unit tests are colocated `_test.go` files.

## Key conventions

- Data flow: handler → `query.Executor` → `query.Translator` → `connection.Manager` (DuckDB).
- Table naming: `{database}.{schema}_{table}` in legacy mode; `"db"."schema"."table"` in catalog mode (`ENABLE_CATALOG_MODE=true`). See `pkg/query/table_naming.go`.
- Executor↔processor circular dependency is resolved post-construction: `executor.Configure(WithCopyProcessor(...), ...)`.
- Streams/CDC use shadow changelog tables `_stream_{name}_changelog`; DML is intercepted in `executeRaw()`. See `design_decisions.md`.
- Env vars: `PORT` (8080), `DB_PATH` (`:memory:`), `STAGE_DIR` (`./stages`), `ENABLE_CATALOG_MODE` (`false`).

## Checks before submitting

Run `make lint` and `make test-all`. CI (`.github/workflows/ci.yaml`) runs lint → all tests → build + docker.
