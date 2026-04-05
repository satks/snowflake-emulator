//nolint:errcheck,gosec,govet,bodyclose // Test file with simplified error handling
package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/pkg/session"
	"github.com/nnnkkk7/snowflake-emulator/pkg/stage"
	"github.com/nnnkkk7/snowflake-emulator/server/handlers"
)

// setupCatalogTestServer creates a test server with catalog mode enabled.
func setupCatalogTestServer(t *testing.T) (*httptest.Server, *session.Manager, *metadata.Repository) {
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

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	sessionMgr := session.NewManager(1 * time.Hour)
	executor := query.NewExecutor(mgr, repo, query.WithCatalogMode(true))

	stageDir, err := os.MkdirTemp("", "catalog_test_stages_*")
	if err != nil {
		t.Fatalf("failed to create temp stage dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(stageDir)
	})
	stageMgr := stage.NewManager(repo, stageDir)

	copyProcessor := query.NewCopyProcessor(stageMgr, repo, executor)
	mergeProcessor := query.NewMergeProcessor(executor)
	executor.Configure(
		query.WithCopyProcessor(copyProcessor),
		query.WithMergeProcessor(mergeProcessor),
	)

	// Create test database and schema using catalog mode
	ctx := context.Background()
	database, err := repo.CreateDatabaseCatalog(ctx, "TEST_DB", "Test database", true)
	if err != nil {
		t.Fatalf("failed to create catalog database: %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, database.ID, "PUBLIC", "Public schema", true)
	if err != nil {
		t.Fatalf("failed to create catalog schema: %v", err)
	}

	// Create test table using catalog mode (three-part name)
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "SCORE", Type: "INTEGER"},
	}
	_, err = repo.CreateTableCatalog(ctx, schema.ID, "STUDENTS", columns, "Student data")
	if err != nil {
		t.Fatalf("failed to create catalog table: %v", err)
	}

	// Create handlers with catalog mode
	sessionHandler := handlers.NewSessionHandlerWithCatalogMode(sessionMgr, repo, true)
	queryHandler := handlers.NewQueryHandler(executor, sessionMgr)

	mux := http.NewServeMux()
	mux.HandleFunc("/session/v1/login-request", sessionHandler.Login)
	mux.HandleFunc("/session/renew", sessionHandler.RenewSession)
	mux.HandleFunc("/session/logout", sessionHandler.Logout)
	mux.HandleFunc("/queries/v1/query-request", queryHandler.ExecuteQuery)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server, sessionMgr, repo
}

// TestCatalogIntegration_CompleteWorkflow tests the complete workflow with catalog mode.
func TestCatalogIntegration_CompleteWorkflow(t *testing.T) {
	server, _, _ := setupCatalogTestServer(t)

	// Step 1: Login
	loginReq := map[string]interface{}{
		"data": map[string]interface{}{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}

	body, _ := json.Marshal(loginReq)
	resp, err := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Login request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	resp.Body.Close()

	if loginResp["success"] != true {
		t.Fatalf("Login not successful: %v", loginResp)
	}

	data := loginResp["data"].(map[string]interface{})
	token := data["token"].(string)
	if token == "" {
		t.Fatal("No token returned")
	}

	// Step 2: Insert data using three-part name
	insertReq := map[string]string{
		"sqlText": `INSERT INTO "TEST_DB"."PUBLIC"."STUDENTS" VALUES (1, 'Alice', 95), (2, 'Bob', 85)`,
	}
	body, _ = json.Marshal(insertReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Authorization", `Snowflake Token="`+token+`"`)
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Insert request failed: %v", err)
	}

	var insertResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&insertResp)
	resp.Body.Close()

	if insertResp["success"] != true {
		t.Fatalf("Insert not successful: %v", insertResp)
	}

	// Step 3: Query data with Snowflake function translation
	queryReq := map[string]string{
		"sqlText": `SELECT NAME, IFF(SCORE >= 90, 'A', 'B') AS GRADE FROM "TEST_DB"."PUBLIC"."STUDENTS" ORDER BY ID`,
	}
	body, _ = json.Marshal(queryReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Authorization", `Snowflake Token="`+token+`"`)
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Query request failed: %v", err)
	}

	var queryResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&queryResp)
	resp.Body.Close()

	if queryResp["success"] != true {
		t.Fatalf("Query not successful: %v", queryResp)
	}

	queryData := queryResp["data"].(map[string]interface{})
	rowSet := queryData["rowset"].([]interface{})
	if len(rowSet) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rowSet))
	}
}

// TestCatalogIntegration_QueryWithTranslation tests Snowflake SQL translation in catalog mode.
func TestCatalogIntegration_QueryWithTranslation(t *testing.T) {
	server, _, _ := setupCatalogTestServer(t)

	// Login
	loginReq := map[string]interface{}{
		"data": map[string]interface{}{
			"LOGIN_NAME":   "testuser",
			"PASSWORD":     "testpass",
			"databaseName": "TEST_DB",
			"schemaName":   "PUBLIC",
		},
	}
	body, _ := json.Marshal(loginReq)
	resp, _ := http.Post(server.URL+"/session/v1/login-request", "application/json", bytes.NewReader(body))
	var loginResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&loginResp)
	resp.Body.Close()
	token := loginResp["data"].(map[string]interface{})["token"].(string)

	// Insert data
	insertReq := map[string]string{
		"sqlText": `INSERT INTO "TEST_DB"."PUBLIC"."STUDENTS" VALUES (1, 'Alice', 95), (2, 'Bob', 85)`,
	}
	body, _ = json.Marshal(insertReq)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Authorization", `Snowflake Token="`+token+`"`)
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// Query with NVL translation
	queryReq := map[string]string{
		"sqlText": `SELECT NVL(NAME, 'unknown') AS name FROM "TEST_DB"."PUBLIC"."STUDENTS" WHERE ID = 1`,
	}
	body, _ = json.Marshal(queryReq)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/queries/v1/query-request", bytes.NewReader(body))
	req.Header.Set("Authorization", `Snowflake Token="`+token+`"`)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Query request failed: %v", err)
	}

	var queryResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&queryResp)
	resp.Body.Close()

	if queryResp["success"] != true {
		t.Fatalf("Query not successful: %v", queryResp)
	}

	queryData := queryResp["data"].(map[string]interface{})
	rowSet := queryData["rowset"].([]interface{})
	if len(rowSet) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rowSet))
	}
}
