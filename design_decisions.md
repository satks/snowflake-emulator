# Design Decisions: Snowflake Emulator Gap Closure

**Date:** 2026-04-05 to 2026-04-06
**Purpose:** Document architectural decisions made while closing all 10 gaps from the Snowflake Emulator Gap Analysis to support SignalSmith E2E tests.

---

## 1. Feature Flag for Catalog Mode

**Decision:** Gate catalog-based database management behind `ENABLE_CATALOG_MODE=true` env var.

**Context:** The emulator originally used DuckDB schemas to represent Snowflake databases (flat naming: `DB.SCHEMA_TABLE`). Closing Gap 1+2 required switching to DuckDB catalogs via `ATTACH ':memory:' AS {name}` for real three-part name resolution (`DB.SCHEMA.TABLE`).

**Why feature flag:** Changing the storage model would break all existing tests and users. By adding parallel `*Catalog()` methods alongside existing ones and gating with an env var, we ensure zero regression when the flag is off while enabling new behavior when on.

**Trade-off:** Code duplication between legacy and catalog paths. Acceptable because the emulator is a test tool — correctness matters more than DRY.

**Affected files:** `pkg/config/constants.go`, `pkg/metadata/repository.go`, `pkg/query/executor.go`, `cmd/server/main.go`, `server/handlers/session.go`, `server/handlers/rest_api_v2.go`

---

## 2. DuckDB Transaction Limitations with ATTACH

**Decision:** Split catalog operations into separate non-transactional steps with manual cleanup on failure.

**Context:** DuckDB cannot write to multiple attached databases in a single transaction. When creating a table in an attached catalog, we can't atomically write to both the catalog (CREATE TABLE) and the default `memory` catalog (_metadata_tables INSERT) in one tx.

**Approach:** Execute the DuckDB DDL first, then write metadata separately. On metadata failure, clean up the DuckDB artifact (DROP TABLE/DETACH). This is not fully ACID but acceptable for a test emulator.

**Affected files:** `pkg/metadata/repository.go` (CreateTableCatalog, DropTableCatalog, CreateDatabaseCatalog)

---

## 3. SQL Translator String Fallback

**Decision:** Add regex-based string fallback for function translations when AST parsing fails.

**Context:** The vitess-sqlparser cannot parse three-part quoted names (`"DB"."SCHEMA"."TABLE"`) or certain Snowflake syntax (window functions, `::` cast operator, `INPUT =>` named params). When AST parsing fails, the translator originally returned the SQL unchanged — meaning Snowflake functions like IFF, NVL would pass through untranslated.

**Approach:** `applyStringFallbackTranslations()` applies regex-based replacements for all registered functions. This runs when AST parsing fails, ensuring translations work regardless of SQL complexity.

**Trade-off:** Regex replacements are less precise than AST transformations (could match inside string literals). Acceptable for test workloads where this edge case is rare.

**Affected files:** `pkg/query/translator.go`

---

## 4. SHOW/DESCRIBE Metadata-First with DuckDB Fallback

**Decision:** Intercept SHOW/DESCRIBE in the executor's `Query()` method, answer from metadata repository, fall back to DuckDB native.

**Context:** DuckDB doesn't support `SHOW SCHEMAS` or schema-qualified `SHOW TABLES IN db.schema`. But tables created via raw SQL (not through the metadata repo) wouldn't appear in metadata results.

**Approach:** Try metadata lookup first. If it fails (e.g., table was created via raw SQL without metadata registration), fall back to DuckDB's native `DESCRIBE`. This preserves backward compatibility while adding Snowflake-syntax support.

**Affected files:** `pkg/query/executor.go` (queryShowSchemas, queryShowTables, queryDescribeTable)

---

## 5. Streams/CDC as Shadow Changelog Tables

**Decision:** Emulate Snowflake Streams using shadow changelog tables with executor-level DML interception.

**Context:** DuckDB has no native triggers, streams, or CDC support. SignalSmith E2E tests need `CREATE STREAM`, `METADATA$ACTION/ISUPDATE/ROW_ID` virtual columns, and `SYSTEM$STREAM_HAS_DATA()`.

**Approach:**
- Each stream creates a hidden `_stream_{name}_changelog` table with source columns + metadata columns
- `SHOW_INITIAL_ROWS=TRUE` snapshots existing source data into changelog at creation time
- After every DML (`executeRaw()`), the executor checks if the target table has active streams and writes changelog entries
- `SYSTEM$STREAM_HAS_DATA()` checks `COUNT(*) WHERE _event_id > current_offset`
- Event-based offset tracking via `_event_id` auto-increment and `current_offset` in stream metadata

**Trade-off:** DML interception adds overhead to every write. Acceptable for test workloads. The approach doesn't perfectly replicate Snowflake semantics (e.g., insert-delete nullification, exact UPDATE decomposition) but is sufficient for E2E test verification.

**Affected files:** `pkg/query/stream_processor.go` (new), `pkg/query/executor.go`, `pkg/metadata/repository.go`, `cmd/server/main.go`

---

## 6. Function Translation Strategy

**Decision:** Three-tier translation — AST handlers for parseable SQL, string fallback for unparseable SQL, DDL defaults for CREATE TABLE.

**Registered translations (22 total):**

| Tier | Functions |
|------|-----------|
| **AST + Post-process** | IFF, NVL, NVL2, IFNULL, LISTAGG, FLATTEN, OBJECT_CONSTRUCT, TO_VARIANT, PARSE_JSON, DATEADD, DATEDIFF, UUID_STRING, SHA2, ARRAY_SIZE, ARRAY_CONTAINS, TO_VARCHAR, TRY_TO_DOUBLE, TRY_TO_TIMESTAMP, CONVERT_TIMEZONE, GET_PATH |
| **String fallback** | All simple renames above + `::VARIANT→::JSON`, `TABLE(UNNEST(...))` stripping, `INPUT =>` stripping, `.VALUE` stripping |
| **DDL defaults** | `UUID_STRING()→uuid()`, `CURRENT_TIMESTAMP()→CURRENT_TIMESTAMP`, `::VARIANT→::JSON` |

**Natively supported (no translation):** MD5, REGEXP_REPLACE, ILIKE, COUNT_IF, CONCAT_WS, ROW_NUMBER/RANK with NULLS LAST.

---

## 7. ALTER TABLE CLUSTER BY as No-Op

**Decision:** Silently accept and ignore `ALTER TABLE ... CLUSTER BY` statements.

**Context:** DuckDB doesn't support clustering. Clustering is a Snowflake performance optimization that doesn't affect query correctness.

**Approach:** `IsAlterTableClusterBy()` classifier check in `Execute()` returns `{RowsAffected: 0}` immediately, before any translation or DuckDB execution.

**Affected files:** `pkg/query/classifier.go`, `pkg/query/executor.go`

---

## Gap Closure Summary

All 10 gaps from the Snowflake Emulator Gap Analysis are now closed:

| PR | Gaps | Description |
|----|------|-------------|
| #1 | P0: Gaps 1+2 | Catalog mode (CREATE DATABASE via SQL, three-part names) |
| #2 | P2+P3: Gaps 7+8+9+10 | Function translations, CLUSTER BY no-op, DDL defaults, window functions |
| #3 | P1: Gap 3 | SHOW SCHEMAS/TABLES, DESCRIBE TABLE |
| #4 | P1: Gaps 4+5+6 | VARIANT ops, FLATTEN TABLE(), Streams/CDC |
