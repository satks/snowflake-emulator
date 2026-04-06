<p align="center">
  <img src="assets/snowflake-emulator.png" alt="snowflake-emulator" width="360" />
</p>

The Go gopher was designed by the awesome [Renee French](https://reneefrench.blogspot.com/)

# Snowflake Emulator

A lightweight, open-source Snowflake emulator built with Go and DuckDB, designed for local development and testing.

[![CI](https://github.com/nnnkkk7/snowflake-emulator/workflows/CI/badge.svg)](https://github.com/nnnkkk7/snowflake-emulator/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/nnnkkk7/snowflake-emulator.svg)](https://pkg.go.dev/github.com/nnnkkk7/snowflake-emulator)
[![GitHub Stars](https://img.shields.io/github/stars/nnnkkk7/snowflake-emulator?style=social)](https://github.com/nnnkkk7/snowflake-emulator)

⭐ Like it? Give us a star!

## TL;DR

```bash
docker run -p 8080:8080 ghcr.io/nnnkkk7/snowflake-emulator:latest
```

```go
dsn := "user:pass@localhost:8080/TEST_DB/PUBLIC?account=test&protocol=http"
db, _ := sql.Open("snowflake", dsn)
rows, _ := db.Query("SELECT IFF(1>0,'yes','no')")  // Snowflake SQL works!
```

## Use Cases

- Run integration tests locally without Snowflake credentials (Go via gosnowflake, or any language via REST API)
- Cheap & fast CI smoke tests for Snowflake SQL
- Validate Snowflake-ish SQL behavior before hitting real Snowflake

> **Note**: This is a dev/test emulator — no auth, no clustering, no external stages, no JS stored procedures. See [Limitations](#limitations) for details.

## Overview

Snowflake Emulator provides a [Snowflake](https://www.snowflake.com/)-compatible SQL interface backed by DuckDB for local development and testing:

- **Local & CI workflows** - Run Snowflake-compatible SQL with no external dependencies
- **Snowflake-compatible access** - [`gosnowflake`](https://github.com/snowflakedb/gosnowflake) driver support and REST API v2
- **SQL execution** - Snowflake → DuckDB translation

## Installation

<details>
<summary><b>Platform Support</b></summary>

| Platform | Docker | Binary |
|----------|--------|--------|
| Linux x86_64 (amd64) | ✅ | ✅ |
| Linux ARM64 | ✅ | - |
| macOS x86_64 | ✅ | - |
| macOS ARM64 (Apple Silicon) | ✅ | - |
| Windows (WSL2) | ✅ | - |

> **Note**: Binary releases are only available for Linux x86_64. This is due to DuckDB requiring CGO, which makes cross-compilation complex. For all other platforms, Docker is recommended.

</details>

### Docker (Recommended)

Docker is the recommended installation method for all platforms.

```bash
# Pull the image
docker pull ghcr.io/nnnkkk7/snowflake-emulator:latest

# Run with in-memory database
docker run -p 8080:8080 ghcr.io/nnnkkk7/snowflake-emulator:latest

# Run with persistent storage
docker run -p 8080:8080 -v snowflake-data:/data \
  -e DB_PATH=/data/snowflake.db \
  ghcr.io/nnnkkk7/snowflake-emulator:latest
```

### Build from Source (Linux x86_64)

Prerequisites:

- Go 1.24+
- GCC (for DuckDB CGO)

```bash
git clone https://github.com/nnnkkk7/snowflake-emulator.git
cd snowflake-emulator
CGO_ENABLED=1 go build -o snowflake-emulator ./cmd/server
```

### Run the Server

```bash
# In-memory mode (default)
./snowflake-emulator

# With persistent storage
DB_PATH=/path/to/database.db ./snowflake-emulator

# Custom port
PORT=9090 ./snowflake-emulator
```

### Using with gosnowflake Driver

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/snowflakedb/gosnowflake"
)

func main() {
    // Connect to local emulator
    dsn := "user:pass@localhost:8080/TEST_DB/PUBLIC?account=test&protocol=http"
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Execute Snowflake SQL (automatically translated)
    rows, err := db.Query(`
        SELECT
            name,
            IFF(score >= 90, 'A', 'B') AS grade,
            NVL(email, 'no-email') AS email
        FROM users
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var name, grade, email string
        rows.Scan(&name, &grade, &email)
        fmt.Printf("%s: %s (%s)\n", name, grade, email)
    }
}
```

### Using REST API v2

```bash
# Submit a SQL statement
curl -X POST http://localhost:8080/api/v2/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "SELECT IFF(1 > 0, '\''yes'\'', '\''no'\'')",
    "database": "TEST_DB",
    "schema": "PUBLIC"
  }'

# Get statement result
curl http://localhost:8080/api/v2/statements/{handle}

# Create a database
curl -X POST http://localhost:8080/api/v2/databases \
  -H "Content-Type: application/json" \
  -d '{"name": "MY_DB"}'

# List warehouses
curl http://localhost:8080/api/v2/warehouses
```

## Next Steps

| Example | Description |
|---------|-------------|
| [`gosnowflake/`](example/gosnowflake/) | Go driver example |
| [`restapi/`](example/restapi/) | REST API v2 example |
| [`docker/`](example/docker/) | Docker container example |

```bash
# Start the emulator
go run ./cmd/server

# In another terminal, run an example
go run ./example/gosnowflake
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `DB_PATH` | `:memory:` | DuckDB database path (empty for in-memory) |
| `STAGE_DIR` | `./stages` | Directory for internal stage files |
| `ENABLE_CATALOG_MODE` | `false` | When `true`, databases use DuckDB ATTACH catalogs enabling three-part name resolution (`db.schema.table`) and CREATE/DROP DATABASE via SQL |

## API Endpoints

### gosnowflake Protocol

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/session/v1/login-request` | POST | Session login |
| `/session/token-request` | POST | Token refresh |
| `/session/heartbeat` | POST | Keep-alive |
| `/session/renew` | POST | Renew session |
| `/session/logout` | POST | Logout |
| `/session/use` | POST | USE DATABASE/SCHEMA |
| `/queries/v1/query-request` | POST | Execute SQL query |
| `/queries/v1/abort-request` | POST | Cancel query |

### REST API v2

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v2/statements` | POST | Submit SQL statement |
| `/api/v2/statements/{handle}` | GET | Get statement status/result |
| `/api/v2/statements/{handle}/cancel` | POST | Cancel statement |
| `/api/v2/databases` | GET, POST | List/Create databases |
| `/api/v2/databases/{db}` | GET, PUT, DELETE | Get/Alter/Drop database |
| `/api/v2/databases/{db}/schemas` | GET, POST | List/Create schemas |
| `/api/v2/databases/{db}/schemas/{schema}` | GET, DELETE | Get/Drop schema |
| `/api/v2/databases/{db}/schemas/{schema}/tables` | GET, POST | List/Create tables |
| `/api/v2/databases/{db}/schemas/{schema}/tables/{table}` | GET, PUT, DELETE | Get/Alter/Drop table |
| `/api/v2/warehouses` | GET, POST | List/Create warehouses |
| `/api/v2/warehouses/{wh}` | GET, DELETE | Get/Drop warehouse |
| `/api/v2/warehouses/{wh}:resume` | POST | Resume warehouse |
| `/api/v2/warehouses/{wh}:suspend` | POST | Suspend warehouse |
| `/health` | GET | Health check |

## Compatibility

<details>
<summary><b>Supported SQL Operations</b></summary>

The emulator supports standard SQL operations with automatic Snowflake-to-DuckDB translation:

| Category | Operations | Description |
|----------|------------|-------------|
| **Query** | `SELECT`, `EXPLAIN` | Read operations with full result set support |
| **Query** | `SHOW SCHEMAS`, `SHOW TABLES` | List metadata objects (supports `IN DATABASE`/`IN schema` qualifiers) |
| **Query** | `DESCRIBE TABLE` / `DESC TABLE` | Inspect table columns and types |
| **DML** | `INSERT`, `UPDATE`, `DELETE` | Data manipulation with rows affected count |
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` | Schema management |
| **DDL** | `CREATE DATABASE`, `DROP DATABASE` | Database management |
| **DDL** | `CREATE SCHEMA`, `DROP SCHEMA` | Schema namespace management |
| **DDL** | `ALTER TABLE ... CLUSTER BY` | Accepted as no-op (DuckDB has no clustering) |
| **Streams** | `CREATE STREAM`, `DROP STREAM` | Change Data Capture on tables (see [Streams/CDC](#streamscdc)) |
| **Streams** | `SYSTEM$STREAM_HAS_DATA()` | Check if a stream has unconsumed changes |
| **Transaction** | `BEGIN`, `COMMIT`, `ROLLBACK` | Transaction control |
| **Session** | `USE DATABASE`, `USE SCHEMA` | Set active database/schema context |
| **Data Loading** | `COPY INTO` | Bulk data loading from internal stages (CSV, JSON) |
| **Upsert** | `MERGE INTO` | Conditional insert/update/delete operations |

**Parameter Binding**: Supports positional placeholder substitution (`:1`, `:2`, `?`).

</details>

<details>
<summary><b>Supported SQL Functions</b></summary>

| Snowflake | DuckDB | Description |
|-----------|--------|-------------|
| `IFF(cond, t, f)` | `IF(cond, t, f)` | Conditional expression |
| `NVL(a, b)` | `COALESCE(a, b)` | Null value substitution |
| `NVL2(a, b, c)` | `IF(a IS NOT NULL, b, c)` | Null conditional |
| `IFNULL(a, b)` | `COALESCE(a, b)` | Null value substitution |
| `DATEADD(part, n, date)` | `date + INTERVAL n part` | Date arithmetic |
| `DATEDIFF(part, start, end)` | `DATE_DIFF('part', start, end)` | Date difference |
| `TO_VARIANT(x)` | `CAST(x AS JSON)` | Convert to variant |
| `PARSE_JSON(str)` | `CAST(str AS JSON)` | Parse JSON string |
| `OBJECT_CONSTRUCT(...)` | `json_object(...)` | Build JSON object |
| `LISTAGG(col, sep)` | `STRING_AGG(col, sep)` | String aggregation |
| `FLATTEN(...)` | `UNNEST(...)` | Array expansion |
| `UUID_STRING()` | `uuid()` | Generate UUID |
| `ARRAY_SIZE(arr)` | `len(arr)` | Array length |
| `TO_VARCHAR(ts, fmt)` | `strftime(ts, fmt)` | Format value to string |
| `SHA2(expr, bits)` | `sha256(expr)` | Hash function |
| `ARRAY_CONTAINS(val, arr)` | `list_contains(arr, val)` | Check array membership |
| `TRY_TO_DOUBLE(expr)` | `TRY_CAST(expr AS DOUBLE)` | Safe cast to double |
| `TRY_TO_TIMESTAMP(expr)` | `TRY_CAST(expr AS TIMESTAMP)` | Safe cast to timestamp |
| `CONVERT_TIMEZONE(from, to, ts)` | `timezone(to, ts)` | Timezone conversion |
| `GET_PATH(obj, path)` | `json_extract_string(obj, path)` | JSON path extraction |
| `::VARIANT` cast | `::JSON` cast | Variant type cast |

</details>

<details>
<summary><b>Supported Data Types</b></summary>

| Snowflake Type | DuckDB Type |
|----------------|-------------|
| NUMBER, NUMERIC, DECIMAL | DOUBLE / DECIMAL(p,s) |
| INTEGER, BIGINT, SMALLINT, TINYINT | INTEGER / BIGINT |
| FLOAT, DOUBLE, REAL | DOUBLE |
| VARCHAR, STRING, TEXT, CHAR | VARCHAR |
| BOOLEAN | BOOLEAN |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP, TIMESTAMP_NTZ | TIMESTAMP |
| TIMESTAMP_LTZ, TIMESTAMP_TZ | TIMESTAMPTZ |
| VARIANT, OBJECT | JSON |
| ARRAY | JSON |
| BINARY, VARBINARY | BLOB |
| GEOGRAPHY, GEOMETRY | VARCHAR (WKT) |

</details>

## Streams/CDC

The emulator supports Snowflake Streams for Change Data Capture (CDC). Streams track DML changes (`INSERT`, `UPDATE`, `DELETE`) on source tables using shadow changelog tables.

```sql
-- Create a stream on a table
CREATE STREAM my_stream ON TABLE my_table;

-- Query the stream for changes (includes METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
SELECT * FROM my_stream;

-- Check if stream has unconsumed data
SELECT SYSTEM$STREAM_HAS_DATA('my_stream');

-- Drop stream
DROP STREAM my_stream;
```

Options: `APPEND_ONLY = TRUE/FALSE`, `SHOW_INITIAL_ROWS = TRUE/FALSE`.

## Limitations

This emulator is designed for development and testing. The following features are not supported:

- Authentication/Authorization (skipped in dev mode)
- Distributed processing / Clustering
- Time Travel / Zero-Copy Cloning
- Tasks, Pipes
- External stages (S3, Azure, GCS)
- Stored procedures with JavaScript
- User-defined functions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT](LICENSE)

