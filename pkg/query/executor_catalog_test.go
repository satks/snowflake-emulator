package query

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// setupCatalogTestExecutor creates a test executor with catalog mode enabled.
func setupCatalogTestExecutor(t *testing.T) (*Executor, *metadata.Repository) {
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

	executor := NewExecutor(mgr, repo, WithCatalogMode(true))
	return executor, repo
}

// setupCatalogTestDB creates a database, schema, and table using catalog mode.
func setupCatalogTestDB(t *testing.T, repo *metadata.Repository, dbName, schemaName, tableName string, columns []metadata.ColumnDef) {
	t.Helper()
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, dbName, "", true)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog(%s) error = %v", dbName, err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, schemaName, "", true)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog(%s) error = %v", schemaName, err)
	}

	_, err = repo.CreateTableCatalog(ctx, schema.ID, tableName, columns, "")
	if err != nil {
		t.Fatalf("CreateTableCatalog(%s) error = %v", tableName, err)
	}
}

// TestCatalogExecutor_ExecuteQuery tests basic query execution with three-part names.
func TestCatalogExecutor_ExecuteQuery(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "AGE", Type: "INTEGER"},
	}
	setupCatalogTestDB(t, repo, "TEST_DB", "PUBLIC", "USERS", columns)

	// Insert test data using three-part name
	insertSQL := `INSERT INTO "TEST_DB"."PUBLIC"."USERS" VALUES (1, 'Alice', 30), (2, 'Bob', 25)`
	_, err := executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Insert error = %v", err)
	}

	// Test simple SELECT with three-part name
	selectSQL := `SELECT * FROM "TEST_DB"."PUBLIC"."USERS" ORDER BY ID`
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 && len(result.Rows[0]) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Rows[0]))
	}
}

// TestCatalogExecutor_ExecuteWithTranslation tests Snowflake SQL translation with catalog mode.
func TestCatalogExecutor_ExecuteWithTranslation(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "AGE", Type: "INTEGER"},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
	}
	setupCatalogTestDB(t, repo, "TEST_DB", "PUBLIC", "USERS", columns)

	insertSQL := `INSERT INTO "TEST_DB"."PUBLIC"."USERS" VALUES (1, 'Alice', 30, 'alice@example.com'), (2, 'Bob', 17, 'bob@example.com')`
	_, err := executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Insert error = %v", err)
	}

	tests := []struct {
		name          string
		sql           string
		expectedRows  int
		expectedCols  int
		checkFirstRow func(*testing.T, []interface{})
	}{
		{
			name:         "IFFTranslation",
			sql:          `SELECT NAME, IFF(AGE >= 18, 'adult', 'minor') AS category FROM "TEST_DB"."PUBLIC"."USERS" ORDER BY ID`,
			expectedRows: 2,
			expectedCols: 2,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				if row[0] != "Alice" {
					t.Errorf("Expected name 'Alice', got %v", row[0])
				}
				if row[1] != "adult" {
					t.Errorf("Expected category 'adult', got %v", row[1])
				}
			},
		},
		{
			name:         "NVLTranslation",
			sql:          `SELECT NAME, NVL(EMAIL, 'no-email') AS email FROM "TEST_DB"."PUBLIC"."USERS" WHERE ID = 2`,
			expectedRows: 1,
			expectedCols: 2,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				if row[0] != "Bob" {
					t.Errorf("Expected name 'Bob', got %v", row[0])
				}
				if row[1] != "bob@example.com" {
					t.Errorf("Expected email 'bob@example.com', got %v", row[1])
				}
			},
		},
		{
			name:         "CONCATTranslation",
			sql:          `SELECT CONCAT(NAME, ' is ', NAME) AS display FROM "TEST_DB"."PUBLIC"."USERS" WHERE ID = 1`,
			expectedRows: 1,
			expectedCols: 1,
			checkFirstRow: func(t *testing.T, row []interface{}) {
				expected := "Alice is Alice"
				if row[0] != expected {
					t.Errorf("Expected '%s', got %v", expected, row[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Query(ctx, tt.sql)
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}

			if len(result.Rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(result.Rows))
			}

			if len(result.Rows) > 0 && len(result.Rows[0]) != tt.expectedCols {
				t.Errorf("Expected %d columns, got %d", tt.expectedCols, len(result.Rows[0]))
			}

			if tt.checkFirstRow != nil && len(result.Rows) > 0 {
				tt.checkFirstRow(t, result.Rows[0])
			}
		})
	}
}

// TestCatalogExecutor_DDLOperations tests CREATE/DROP DATABASE and TABLE via SQL.
func TestCatalogExecutor_DDLOperations(t *testing.T) {
	executor, _ := setupCatalogTestExecutor(t)
	ctx := context.Background()

	// Create database via SQL
	_, err := executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS DDL_TEST")
	if err != nil {
		t.Fatalf("CREATE DATABASE error = %v", err)
	}

	// Create schema via SQL
	_, err = executor.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS DDL_TEST.DDL_SCHEMA")
	if err != nil {
		t.Fatalf("CREATE SCHEMA error = %v", err)
	}

	// Create table via SQL with three-part name
	createTableSQL := `CREATE TABLE "DDL_TEST"."DDL_SCHEMA"."EMPLOYEES" (
		ID INTEGER PRIMARY KEY,
		NAME VARCHAR NOT NULL,
		SALARY DOUBLE
	)`
	_, err = executor.Execute(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Insert data
	insertSQL := `INSERT INTO "DDL_TEST"."DDL_SCHEMA"."EMPLOYEES" VALUES (1, 'John', 50000.0)`
	_, err = executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	// Query data
	selectSQL := `SELECT NAME, SALARY FROM "DDL_TEST"."DDL_SCHEMA"."EMPLOYEES" WHERE ID = 1`
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Drop table
	_, err = executor.Execute(ctx, `DROP TABLE IF EXISTS "DDL_TEST"."DDL_SCHEMA"."EMPLOYEES"`)
	if err != nil {
		t.Fatalf("DROP TABLE error = %v", err)
	}

	// Drop schema
	_, err = executor.Execute(ctx, "DROP SCHEMA IF EXISTS DDL_TEST.DDL_SCHEMA")
	if err != nil {
		t.Fatalf("DROP SCHEMA error = %v", err)
	}

	// Drop database
	_, err = executor.Execute(ctx, "DROP DATABASE IF EXISTS DDL_TEST")
	if err != nil {
		t.Fatalf("DROP DATABASE error = %v", err)
	}
}

// TestCatalogExecutor_GetColumnInfo tests column metadata retrieval with catalog mode.
func TestCatalogExecutor_GetColumnInfo(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "SALARY", Type: "DOUBLE"},
	}
	setupCatalogTestDB(t, repo, "TEST_DB", "PUBLIC", "EMPLOYEES", columns)

	selectSQL := `SELECT * FROM "TEST_DB"."PUBLIC"."EMPLOYEES" LIMIT 0`
	result, err := executor.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	expectedColumns := []string{"ID", "NAME", "SALARY"}
	if diff := cmp.Diff(expectedColumns, result.Columns); diff != "" {
		t.Errorf("Column names mismatch (-want +got):\n%s", diff)
	}
}

// TestCatalogExecutor_TransactionStatements tests transactions in catalog mode.
func TestCatalogExecutor_TransactionStatements(t *testing.T) {
	executor, _ := setupCatalogTestExecutor(t)
	ctx := context.Background()

	// CREATE DATABASE via catalog mode for transaction test
	_, err := executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS TX_DB")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}
	_, err = executor.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS TX_DB.TX_SCHEMA")
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Create table
	_, err = executor.Execute(ctx, `CREATE TABLE "TX_DB"."TX_SCHEMA"."TX_TEST" (id INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Begin transaction
	_, err = executor.Execute(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert data
	_, err = executor.Execute(ctx, `INSERT INTO "TX_DB"."TX_SCHEMA"."TX_TEST" VALUES (1)`)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Commit
	_, err = executor.Execute(ctx, "COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify data was committed
	result, err := executor.Query(ctx, `SELECT * FROM "TX_DB"."TX_SCHEMA"."TX_TEST" WHERE id = 1`)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row after commit, got %d", len(result.Rows))
	}
}

// TestCatalogExecutor_UseDatabase tests USE DATABASE validation.
func TestCatalogExecutor_UseDatabase(t *testing.T) {
	executor, _ := setupCatalogTestExecutor(t)
	ctx := context.Background()

	// Create database
	_, err := executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS USE_TEST")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// USE DATABASE should succeed for existing database
	_, err = executor.Execute(ctx, "USE DATABASE USE_TEST")
	if err != nil {
		t.Fatalf("USE DATABASE failed: %v", err)
	}

	// USE DATABASE should fail for non-existent database
	_, err = executor.Execute(ctx, "USE DATABASE NONEXISTENT")
	if err == nil {
		t.Error("Expected error for USE DATABASE on non-existent database")
	}
}

// TestCatalogExecutor_QueryWithBindings tests parameter bindings in catalog mode.
func TestCatalogExecutor_QueryWithBindings(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}
	setupCatalogTestDB(t, repo, "TEST_DB", "PUBLIC", "BIND_TEST", columns)

	insertSQL := `INSERT INTO "TEST_DB"."PUBLIC"."BIND_TEST" VALUES (1, 'Alice'), (2, 'Bob')`
	_, err := executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("Insert error = %v", err)
	}

	// Query with binding
	result, err := executor.QueryWithBindings(ctx,
		`SELECT NAME FROM "TEST_DB"."PUBLIC"."BIND_TEST" WHERE ID = :1`,
		map[string]*QueryBindingValue{
			"1": {Type: "FIXED", Value: "1"},
		},
	)
	if err != nil {
		t.Fatalf("QueryWithBindings() error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Rows) > 0 && result.Rows[0][0] != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result.Rows[0][0])
	}
}

// TestCatalogExecutor_ErrorHandling tests error cases in catalog mode.
func TestCatalogExecutor_ErrorHandling(t *testing.T) {
	executor, _ := setupCatalogTestExecutor(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "InvalidSQL",
			sql:     "SELECT FROM",
			wantErr: true,
		},
		{
			name:    "NonExistentCatalog",
			sql:     `SELECT * FROM "NONEXISTENT"."PUBLIC"."TABLE"`,
			wantErr: true,
		},
		{
			name:    "EmptySQL",
			sql:     "",
			wantErr: true,
		},
		{
			name:    "CreateSchemaWithoutDatabase",
			sql:     "CREATE SCHEMA ORPHAN_SCHEMA",
			wantErr: true, // Catalog mode requires db.schema format
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executor.Query(ctx, tt.sql)
			if err == nil {
				// Also try Execute for DDL
				_, err = executor.Execute(ctx, tt.sql)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCatalogExecutor_ShowSchemas tests SHOW SCHEMAS in catalog mode.
func TestCatalogExecutor_ShowSchemas(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabaseCatalog(ctx, "SHOW_DB", "", true)
	_, _ = repo.CreateSchemaCatalog(ctx, db.ID, "ALPHA", "", false)
	_, _ = repo.CreateSchemaCatalog(ctx, db.ID, "BETA", "", false)

	result, err := executor.Query(ctx, `SHOW SCHEMAS IN DATABASE SHOW_DB`)
	if err != nil {
		t.Fatalf("SHOW SCHEMAS error = %v", err)
	}

	// Should include PUBLIC (auto-created) + ALPHA + BETA = 3
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 schemas (PUBLIC+ALPHA+BETA), got %d", len(result.Rows))
	}

	// Verify column names
	if result.Columns[0] != "created_on" || result.Columns[1] != "name" || result.Columns[2] != "database_name" {
		t.Errorf("Unexpected columns: %v", result.Columns)
	}
}

// TestCatalogExecutor_ShowTables tests SHOW TABLES in catalog mode.
func TestCatalogExecutor_ShowTables(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabaseCatalog(ctx, "SHOW_DB", "", true)
	schema, _ := repo.CreateSchemaCatalog(ctx, db.ID, "MY_SCHEMA", "", false)
	cols := []metadata.ColumnDef{{Name: "ID", Type: "INTEGER"}}
	_, _ = repo.CreateTableCatalog(ctx, schema.ID, "TABLE_A", cols, "")
	_, _ = repo.CreateTableCatalog(ctx, schema.ID, "TABLE_B", cols, "")

	result, err := executor.Query(ctx, `SHOW TABLES IN SHOW_DB.MY_SCHEMA`)
	if err != nil {
		t.Fatalf("SHOW TABLES error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(result.Rows))
	}
	if result.Columns[1] != "name" {
		t.Errorf("Expected 'name' column, got %s", result.Columns[1])
	}
}

// TestCatalogExecutor_DescribeTable tests DESCRIBE TABLE in catalog mode.
func TestCatalogExecutor_DescribeTable(t *testing.T) {
	executor, repo := setupCatalogTestExecutor(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabaseCatalog(ctx, "DESC_DB", "", true)
	schema, _ := repo.CreateSchemaCatalog(ctx, db.ID, "MY_SCHEMA", "", false)
	cols := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR", Nullable: true},
		{Name: "SCORE", Type: "DOUBLE"},
	}
	_, _ = repo.CreateTableCatalog(ctx, schema.ID, "STUDENTS", cols, "")

	result, err := executor.Query(ctx, `DESCRIBE TABLE DESC_DB.MY_SCHEMA.STUDENTS`)
	if err != nil {
		t.Fatalf("DESCRIBE TABLE error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 columns described, got %d", len(result.Rows))
	}

	// Check column details
	if len(result.Rows) >= 3 {
		// ID: primary key, not nullable
		if result.Rows[0][0] != "ID" || result.Rows[0][5] != "Y" {
			t.Errorf("Expected ID with PK=Y, got name=%v pk=%v", result.Rows[0][0], result.Rows[0][5])
		}
		// NAME: nullable
		if result.Rows[1][0] != "NAME" || result.Rows[1][3] != "Y" {
			t.Errorf("Expected NAME with null=Y, got name=%v null=%v", result.Rows[1][0], result.Rows[1][3])
		}
	}
}

// TestCatalogExecutor_ShowSchemasBareBare tests bare SHOW SCHEMAS (no database filter).
func TestCatalogExecutor_ShowSchemasBare(t *testing.T) {
	executor, _ := setupCatalogTestExecutor(t)
	ctx := context.Background()

	_, _ = executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS DB1")
	_, _ = executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS DB2")

	result, err := executor.Query(ctx, "SHOW SCHEMAS")
	if err != nil {
		t.Fatalf("SHOW SCHEMAS error = %v", err)
	}
	// DB1 has PUBLIC, DB2 has PUBLIC = at least 2 schemas
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 schemas, got %d", len(result.Rows))
	}
}
