# Contributing

Thanks for your interest in improving Snowflake Emulator! Contributions of all kinds are welcome — bug reports, SQL translation gaps, docs, and code.

## Development setup

Prerequisites: Go 1.24+, GCC (DuckDB requires CGO), and optionally Docker.

```bash
git clone https://github.com/satks/snowflake-emulator.git
cd snowflake-emulator
CGO_ENABLED=1 go build ./...
make run          # starts the server on :8080 (in-memory DuckDB)
```

## Tests and lint

```bash
make test               # unit tests (pkg/...)
make test-integration   # integration tests
make test-e2e           # end-to-end tests (gosnowflake driver + REST API v2)
make test-all           # everything
make lint               # golangci-lint v2
make fmt                # format
```

All tests run with `-race`. Please make sure `make lint` and `make test-all` pass before opening a pull request.

## Pull requests

1. Fork and create a feature branch from `main`.
2. Keep changes focused; include tests for new SQL translations or API behavior.
3. Update `README.md` if you add user-facing features (functions, endpoints, env vars).
4. Open a PR with a clear description of the Snowflake behavior being emulated.

## Reporting issues

When reporting a SQL compatibility issue, please include the Snowflake SQL statement, the expected Snowflake behavior, and the emulator's actual output or error.
