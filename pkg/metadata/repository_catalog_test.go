package metadata

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
)

func setupCatalogTestRepository(t *testing.T) *Repository {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	mgr := connection.NewManager(db)
	repo, err := NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	return repo
}

func TestCreateDatabaseCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "test_db", "test comment", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	if db.Name != "TEST_DB" {
		t.Errorf("expected name TEST_DB, got %s", db.Name)
	}
	if db.Comment != "test comment" {
		t.Errorf("expected comment 'test comment', got %s", db.Comment)
	}

	// Verify we can retrieve it
	retrieved, err := repo.GetDatabaseByName(ctx, "TEST_DB")
	if err != nil {
		t.Fatalf("GetDatabaseByName failed: %v", err)
	}
	if retrieved.ID != db.ID {
		t.Errorf("retrieved ID mismatch")
	}
}

func TestCreateDatabaseCatalog_IfNotExists(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db1, err := repo.CreateDatabaseCatalog(ctx, "test_db", "first", false)
	if err != nil {
		t.Fatalf("first CreateDatabaseCatalog failed: %v", err)
	}

	// Second call with ifNotExists=true should return existing
	db2, err := repo.CreateDatabaseCatalog(ctx, "test_db", "second", true)
	if err != nil {
		t.Fatalf("second CreateDatabaseCatalog failed: %v", err)
	}

	if db1.ID != db2.ID {
		t.Errorf("expected same ID, got %s and %s", db1.ID, db2.ID)
	}

	// Without ifNotExists should fail
	_, err = repo.CreateDatabaseCatalog(ctx, "test_db", "third", false)
	if err == nil {
		t.Error("expected error for duplicate without ifNotExists")
	}
}

func TestDropDatabaseCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	_, err := repo.CreateDatabaseCatalog(ctx, "drop_me", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	err = repo.DropDatabaseCatalog(ctx, "drop_me", false)
	if err != nil {
		t.Fatalf("DropDatabaseCatalog failed: %v", err)
	}

	// Verify it's gone
	_, err = repo.GetDatabaseByName(ctx, "DROP_ME")
	if err == nil {
		t.Error("expected error after drop, database still exists")
	}
}

func TestDropDatabaseCatalog_IfExists(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	// Drop non-existent with ifExists=true should not error
	err := repo.DropDatabaseCatalog(ctx, "nonexistent", true)
	if err != nil {
		t.Fatalf("DropDatabaseCatalog ifExists should not fail: %v", err)
	}

	// Drop non-existent without ifExists should error
	err = repo.DropDatabaseCatalog(ctx, "nonexistent", false)
	if err == nil {
		t.Error("expected error for non-existent without ifExists")
	}
}

func TestCreateSchemaCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "schema_test", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, "my_schema", "test schema", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog failed: %v", err)
	}

	if schema.Name != "MY_SCHEMA" {
		t.Errorf("expected name MY_SCHEMA, got %s", schema.Name)
	}

	// Verify we can retrieve it
	retrieved, err := repo.GetSchemaByName(ctx, db.ID, "MY_SCHEMA")
	if err != nil {
		t.Fatalf("GetSchemaByName failed: %v", err)
	}
	if retrieved.ID != schema.ID {
		t.Errorf("retrieved ID mismatch")
	}
}

func TestCreateSchemaCatalog_IfNotExists(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "schema_ine", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	s1, err := repo.CreateSchemaCatalog(ctx, db.ID, "my_schema", "", false)
	if err != nil {
		t.Fatalf("first CreateSchemaCatalog failed: %v", err)
	}

	s2, err := repo.CreateSchemaCatalog(ctx, db.ID, "my_schema", "", true)
	if err != nil {
		t.Fatalf("second CreateSchemaCatalog with ifNotExists failed: %v", err)
	}

	if s1.ID != s2.ID {
		t.Errorf("expected same ID, got %s and %s", s1.ID, s2.ID)
	}
}

func TestDropSchemaCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "drop_schema_test", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, "to_drop", "", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog failed: %v", err)
	}

	err = repo.DropSchemaCatalog(ctx, schema.ID, false)
	if err != nil {
		t.Fatalf("DropSchemaCatalog failed: %v", err)
	}

	// Verify it's gone
	_, err = repo.GetSchemaByName(ctx, db.ID, "TO_DROP")
	if err == nil {
		t.Error("expected error after drop, schema still exists")
	}
}

func TestCreateTableCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "table_test", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, "my_schema", "", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog failed: %v", err)
	}

	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR", Nullable: true},
	}

	table, err := repo.CreateTableCatalog(ctx, schema.ID, "users", columns, "user table")
	if err != nil {
		t.Fatalf("CreateTableCatalog failed: %v", err)
	}

	if table.Name != "USERS" {
		t.Errorf("expected name USERS, got %s", table.Name)
	}

	// Verify we can query the actual DuckDB table with three-part name
	mgr := repo.mgr
	rows, err := mgr.Query(ctx, `SELECT * FROM "TABLE_TEST"."MY_SCHEMA"."USERS"`)
	if err != nil {
		t.Fatalf("query with three-part name failed: %v", err)
	}
	rows.Close()
}

func TestDropTableCatalog(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	db, err := repo.CreateDatabaseCatalog(ctx, "drop_table_test", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, "my_schema", "", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog failed: %v", err)
	}

	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER"},
	}

	table, err := repo.CreateTableCatalog(ctx, schema.ID, "to_drop", columns, "")
	if err != nil {
		t.Fatalf("CreateTableCatalog failed: %v", err)
	}

	err = repo.DropTableCatalog(ctx, table.ID)
	if err != nil {
		t.Fatalf("DropTableCatalog failed: %v", err)
	}

	// Verify it's gone from metadata
	_, err = repo.GetTable(ctx, table.ID)
	if err == nil {
		t.Error("expected error after drop, table still exists in metadata")
	}
}

func TestCatalogMode_FullWorkflow(t *testing.T) {
	repo := setupCatalogTestRepository(t)
	ctx := context.Background()

	// Create database
	db, err := repo.CreateDatabaseCatalog(ctx, "workflow_db", "", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog failed: %v", err)
	}

	// Create schema
	schema, err := repo.CreateSchemaCatalog(ctx, db.ID, "app_schema", "", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog failed: %v", err)
	}

	// Create table
	columns := []ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "VALUE", Type: "VARCHAR"},
	}
	_, err = repo.CreateTableCatalog(ctx, schema.ID, "data", columns, "")
	if err != nil {
		t.Fatalf("CreateTableCatalog failed: %v", err)
	}

	// Insert data using DuckDB three-part name
	mgr := repo.mgr
	_, err = mgr.Exec(ctx, `INSERT INTO "WORKFLOW_DB"."APP_SCHEMA"."DATA" VALUES (1, 'hello'), (2, 'world')`)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query data
	rows, err := mgr.Query(ctx, `SELECT id, value FROM "WORKFLOW_DB"."APP_SCHEMA"."DATA" ORDER BY id`)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// Drop database (cascades)
	err = repo.DropDatabaseCatalog(ctx, "workflow_db", false)
	if err != nil {
		t.Fatalf("DropDatabaseCatalog failed: %v", err)
	}

	// Verify database is gone
	_, err = repo.GetDatabaseByName(ctx, "WORKFLOW_DB")
	if err == nil {
		t.Error("expected error after drop")
	}
}
