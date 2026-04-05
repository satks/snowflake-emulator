// tests/e2e/catalog_mode_test.go - E2E tests for catalog mode (ENABLE_CATALOG_MODE=true)
//
// These tests verify that three-part naming (database.schema.table) works
// when catalog mode is enabled via DuckDB ATTACH.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
	_ "github.com/snowflakedb/gosnowflake"
)

// setupCatalogModeEmulator creates an in-process emulator with catalog mode enabled.
func setupCatalogModeEmulator(t *testing.T) *httptest.Server {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close DB: %v", err)
		}
	})

	connMgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(connMgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	sessionMgr := session.NewManager(1 * time.Hour)
	executor := query.NewExecutor(connMgr, repo, query.WithCatalogMode(true))

	mergeProcessor := query.NewMergeProcessor(executor)
	executor.Configure(query.WithMergeProcessor(mergeProcessor))

	sessionHandler := handlers.NewSessionHandlerWithCatalogMode(sessionMgr, repo, true)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

	r := chi.NewRouter()
	r.Post("/session", sessionHandler.CloseSession)
	r.Post("/session/v1/login-request", func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		sessionHandler.Login(w, req)
	})
	r.Post("/session/token-request", sessionHandler.TokenRequest)
	r.Post("/session/heartbeat", sessionHandler.Heartbeat)
	r.Post("/queries/v1/query-request", queryHandler.ExecuteQuery)
	r.Post("/queries/v1/abort-request", queryHandler.AbortQuery)
	r.Post("/telemetry/send", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	return server
}

func TestCatalogMode_CreateDatabaseAndThreePartNames(t *testing.T) {
	server := setupCatalogModeEmulator(t)
	hostPort := server.URL[7:] // Strip "http://"

	// Connect via gosnowflake. The login will auto-create TEST_DB in catalog mode.
	dsn := fmt.Sprintf("user:pass@%s/TEST_DB/PUBLIC?account=test&protocol=http", hostPort)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open snowflake connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Step 1: CREATE DATABASE via SQL
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS MYDB")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Step 2: CREATE SCHEMA within that database
	_, err = db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS MYDB.E2E_SCHEMA")
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Step 3: CREATE TABLE using three-part name
	_, err = db.ExecContext(ctx, `CREATE TABLE "MYDB"."E2E_SCHEMA"."USERS" (id INTEGER, name VARCHAR)`)
	if err != nil {
		t.Fatalf("CREATE TABLE with three-part name failed: %v", err)
	}

	// Step 4: INSERT using three-part name
	_, err = db.ExecContext(ctx, `INSERT INTO "MYDB"."E2E_SCHEMA"."USERS" VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("INSERT with three-part name failed: %v", err)
	}

	// Step 5: SELECT using three-part name
	rows, err := db.QueryContext(ctx, `SELECT id, name FROM "MYDB"."E2E_SCHEMA"."USERS" ORDER BY id`)
	if err != nil {
		t.Fatalf("SELECT with three-part name failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		ID   int
		Name string
	}
	for rows.Next() {
		var r struct {
			ID   int
			Name string
		}
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[1].Name != "Bob" {
		t.Errorf("unexpected results: %+v", results)
	}

	// Step 6: DROP TABLE
	_, err = db.ExecContext(ctx, `DROP TABLE IF EXISTS "MYDB"."E2E_SCHEMA"."USERS"`)
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	// Step 7: DROP SCHEMA
	_, err = db.ExecContext(ctx, "DROP SCHEMA IF EXISTS MYDB.E2E_SCHEMA")
	if err != nil {
		t.Fatalf("DROP SCHEMA failed: %v", err)
	}

	// Step 8: DROP DATABASE
	_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS MYDB")
	if err != nil {
		t.Fatalf("DROP DATABASE failed: %v", err)
	}
}

func TestCatalogMode_CreateDatabaseIdempotent(t *testing.T) {
	server := setupCatalogModeEmulator(t)
	hostPort := server.URL[7:]

	dsn := fmt.Sprintf("user:pass@%s/TEST_DB/PUBLIC?account=test&protocol=http", hostPort)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open snowflake connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// CREATE DATABASE twice with IF NOT EXISTS should succeed
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS IDEMPOTENT_DB")
	if err != nil {
		t.Fatalf("First CREATE DATABASE failed: %v", err)
	}

	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS IDEMPOTENT_DB")
	if err != nil {
		t.Fatalf("Second CREATE DATABASE IF NOT EXISTS should not fail: %v", err)
	}

	// Cleanup
	_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS IDEMPOTENT_DB")
}

func TestCatalogMode_MultipleSchemas(t *testing.T) {
	server := setupCatalogModeEmulator(t)
	hostPort := server.URL[7:]

	dsn := fmt.Sprintf("user:pass@%s/TEST_DB/PUBLIC?account=test&protocol=http", hostPort)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open snowflake connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Create database and multiple schemas
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS MULTI_DB")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	_, err = db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS MULTI_DB.SCHEMA_A")
	if err != nil {
		t.Fatalf("CREATE SCHEMA A failed: %v", err)
	}

	_, err = db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS MULTI_DB.SCHEMA_B")
	if err != nil {
		t.Fatalf("CREATE SCHEMA B failed: %v", err)
	}

	// Create tables in different schemas
	_, err = db.ExecContext(ctx, `CREATE TABLE "MULTI_DB"."SCHEMA_A"."ITEMS" (id INTEGER, val VARCHAR)`)
	if err != nil {
		t.Fatalf("CREATE TABLE in SCHEMA_A failed: %v", err)
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE "MULTI_DB"."SCHEMA_B"."ITEMS" (id INTEGER, val VARCHAR)`)
	if err != nil {
		t.Fatalf("CREATE TABLE in SCHEMA_B failed: %v", err)
	}

	// Insert different data in each
	_, err = db.ExecContext(ctx, `INSERT INTO "MULTI_DB"."SCHEMA_A"."ITEMS" VALUES (1, 'A1')`)
	if err != nil {
		t.Fatalf("INSERT into SCHEMA_A failed: %v", err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO "MULTI_DB"."SCHEMA_B"."ITEMS" VALUES (2, 'B1')`)
	if err != nil {
		t.Fatalf("INSERT into SCHEMA_B failed: %v", err)
	}

	// Query from SCHEMA_A
	var val string
	err = db.QueryRowContext(ctx, `SELECT val FROM "MULTI_DB"."SCHEMA_A"."ITEMS" WHERE id = 1`).Scan(&val)
	if err != nil {
		t.Fatalf("SELECT from SCHEMA_A failed: %v", err)
	}
	if val != "A1" {
		t.Errorf("expected A1, got %s", val)
	}

	// Query from SCHEMA_B
	err = db.QueryRowContext(ctx, `SELECT val FROM "MULTI_DB"."SCHEMA_B"."ITEMS" WHERE id = 2`).Scan(&val)
	if err != nil {
		t.Fatalf("SELECT from SCHEMA_B failed: %v", err)
	}
	if val != "B1" {
		t.Errorf("expected B1, got %s", val)
	}

	// Cleanup
	_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS MULTI_DB")
}
