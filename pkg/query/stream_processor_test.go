package query

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// setupStreamTestEnv creates a test environment for stream tests.
func setupStreamTestEnv(t *testing.T) (*Executor, *metadata.Repository, *StreamProcessor) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo)
	streamProcessor := NewStreamProcessor(mgr, repo)
	executor.Configure(WithStreamProcessor(streamProcessor))

	return executor, repo, streamProcessor
}

// TestStreamProcessor_CreateAndDropStream tests the full stream lifecycle.
func TestStreamProcessor_CreateAndDropStream(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	// Setup: create database, schema, and source table
	db, _ := repo.CreateDatabase(ctx, "STREAM_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "VALUE", Type: "DOUBLE"},
	}
	_, _ = repo.CreateTable(ctx, schema.ID, "SOURCE_TABLE", columns, "")

	// Create stream via executor
	_, err := executor.Execute(ctx, `CREATE STREAM IF NOT EXISTS PUBLIC.CDC_STREAM ON TABLE PUBLIC.SOURCE_TABLE APPEND_ONLY = FALSE SHOW_INITIAL_ROWS = TRUE`)
	if err != nil {
		t.Fatalf("CREATE STREAM failed: %v", err)
	}

	// Verify stream exists in metadata
	stream, err := repo.GetStreamByName(ctx, schema.ID, "CDC_STREAM")
	if err != nil {
		t.Fatalf("GetStreamByName failed: %v", err)
	}
	if stream.Name != "CDC_STREAM" {
		t.Errorf("Expected stream name CDC_STREAM, got %s", stream.Name)
	}
	if stream.AppendOnly {
		t.Error("Expected AppendOnly=false")
	}
	if !stream.ShowInitialRows {
		t.Error("Expected ShowInitialRows=true")
	}

	// Drop stream
	_, err = executor.Execute(ctx, `DROP STREAM IF EXISTS PUBLIC.CDC_STREAM`)
	if err != nil {
		t.Fatalf("DROP STREAM failed: %v", err)
	}

	// Verify stream is gone
	_, err = repo.GetStreamByName(ctx, schema.ID, "CDC_STREAM")
	if err == nil {
		t.Error("Expected error after drop, stream still exists")
	}
}

// TestStreamProcessor_StreamHasData tests SYSTEM$STREAM_HAS_DATA function.
func TestStreamProcessor_StreamHasData(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	// Setup
	db, _ := repo.CreateDatabase(ctx, "STREAM_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}
	_, _ = repo.CreateTable(ctx, schema.ID, "DATA_TABLE", columns, "")

	// Insert data into source BEFORE creating stream with SHOW_INITIAL_ROWS
	insertSQL := "INSERT INTO STREAM_DB.PUBLIC_DATA_TABLE VALUES (1, 'Alice'), (2, 'Bob')"
	_, err := executor.Execute(ctx, insertSQL)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create stream with SHOW_INITIAL_ROWS=TRUE (should snapshot existing data)
	_, err = executor.Execute(ctx, `CREATE STREAM PUBLIC.HAS_DATA_STREAM ON TABLE PUBLIC.DATA_TABLE SHOW_INITIAL_ROWS = TRUE`)
	if err != nil {
		t.Fatalf("CREATE STREAM failed: %v", err)
	}

	// SYSTEM$STREAM_HAS_DATA should return true (initial rows were snapshotted)
	result, err := executor.Query(ctx, `SELECT SYSTEM$STREAM_HAS_DATA('HAS_DATA_STREAM') AS has_data`)
	if err != nil {
		t.Fatalf("SYSTEM$STREAM_HAS_DATA failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != true {
		t.Errorf("Expected has_data=true, got %v", result.Rows[0][0])
	}

	// Cleanup
	_, _ = executor.Execute(ctx, `DROP STREAM IF EXISTS PUBLIC.HAS_DATA_STREAM`)
}

// TestStreamProcessor_DropStreamIfExists tests DROP STREAM IF EXISTS on non-existent stream.
func TestStreamProcessor_DropStreamIfExists(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "STREAM_DB", "")
	_, _ = repo.CreateSchema(ctx, db.ID, "PUBLIC", "")

	// DROP STREAM IF EXISTS on non-existent stream should not error
	_, err := executor.Execute(ctx, `DROP STREAM IF EXISTS PUBLIC.NONEXISTENT_STREAM`)
	if err != nil {
		t.Fatalf("DROP STREAM IF EXISTS should not fail: %v", err)
	}
}

// TestStreamProcessor_CreateStreamIdempotent tests IF NOT EXISTS behavior.
func TestStreamProcessor_CreateStreamIdempotent(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "STREAM_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{{Name: "ID", Type: "INTEGER"}}
	_, _ = repo.CreateTable(ctx, schema.ID, "SRC", columns, "")

	// Create stream
	_, err := executor.Execute(ctx, `CREATE STREAM IF NOT EXISTS PUBLIC.MY_STREAM ON TABLE PUBLIC.SRC`)
	if err != nil {
		t.Fatalf("First CREATE STREAM failed: %v", err)
	}

	// Create again with IF NOT EXISTS — should not error
	_, err = executor.Execute(ctx, `CREATE STREAM IF NOT EXISTS PUBLIC.MY_STREAM ON TABLE PUBLIC.SRC`)
	if err != nil {
		t.Fatalf("Second CREATE STREAM IF NOT EXISTS should not fail: %v", err)
	}
}

// TestParseCreateStream tests the CREATE STREAM parser.
func TestParseCreateStream_Basic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *CreateStreamStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  `CREATE STREAM PUBLIC.CDC_STREAM ON TABLE PUBLIC.SOURCE_TABLE`,
			want: &CreateStreamStmt{Schema: "PUBLIC", Name: "CDC_STREAM", SourceSchema: "PUBLIC", SourceTable: "SOURCE_TABLE"},
		},
		{
			name: "if_not_exists_with_options",
			sql:  `CREATE STREAM IF NOT EXISTS my_stream ON TABLE my_table APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE`,
			want: &CreateStreamStmt{Name: "MY_STREAM", SourceTable: "MY_TABLE", IfNotExists: true, AppendOnly: true, ShowInitialRows: true},
		},
		{
			name: "append_only_false",
			sql:  `CREATE STREAM s1 ON TABLE t1 APPEND_ONLY = FALSE`,
			want: &CreateStreamStmt{Name: "S1", SourceTable: "T1", AppendOnly: false},
		},
		{
			name:    "invalid",
			sql:     "CREATE TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCreateStream(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCreateStream() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Name != tt.want.Name {
					t.Errorf("Name = %s, want %s", got.Name, tt.want.Name)
				}
				if got.SourceTable != tt.want.SourceTable {
					t.Errorf("SourceTable = %s, want %s", got.SourceTable, tt.want.SourceTable)
				}
				if got.AppendOnly != tt.want.AppendOnly {
					t.Errorf("AppendOnly = %v, want %v", got.AppendOnly, tt.want.AppendOnly)
				}
				if got.ShowInitialRows != tt.want.ShowInitialRows {
					t.Errorf("ShowInitialRows = %v, want %v", got.ShowInitialRows, tt.want.ShowInitialRows)
				}
				if got.IfNotExists != tt.want.IfNotExists {
					t.Errorf("IfNotExists = %v, want %v", got.IfNotExists, tt.want.IfNotExists)
				}
			}
		})
	}
}

// TestStream_DMLInterception_Insert tests that INSERT on a tracked table writes to the changelog.
func TestStream_DMLInterception_Insert(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "DML_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{
		{Name: "ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
	}
	_, _ = repo.CreateTable(ctx, schema.ID, "TRACKED", columns, "")

	// Create stream (no initial rows)
	_, err := executor.Execute(ctx, `CREATE STREAM PUBLIC.INS_STREAM ON TABLE PUBLIC.TRACKED`)
	if err != nil {
		t.Fatalf("CREATE STREAM failed: %v", err)
	}

	// INSERT into tracked table — should trigger changelog write
	_, err = executor.Execute(ctx, "INSERT INTO DML_DB.PUBLIC_TRACKED VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify SYSTEM$STREAM_HAS_DATA returns true
	result, err := executor.Query(ctx, `SELECT SYSTEM$STREAM_HAS_DATA('INS_STREAM') AS has_data`)
	if err != nil {
		t.Fatalf("SYSTEM$STREAM_HAS_DATA failed: %v", err)
	}
	if result.Rows[0][0] != true {
		t.Errorf("Expected has_data=true after INSERT, got %v", result.Rows[0][0])
	}

	// Verify changelog has entries — query directly via connection manager to avoid executor interception
	stream, _ := repo.GetStreamByName(ctx, schema.ID, "INS_STREAM")
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, stream.ChangelogTable)
	countRows, err := executor.Query(ctx, countSQL)
	if err != nil {
		t.Fatalf("Changelog count query failed: %v", err)
	}
	if len(countRows.Rows) == 0 {
		t.Fatal("No count result returned")
	}
	count, ok := countRows.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64 count, got %T: %v", countRows.Rows[0][0], countRows.Rows[0][0])
	}
	if count < 2 {
		t.Errorf("Expected at least 2 changelog entries, got %d", count)
	}
}

// TestStream_MetadataRowID verifies METADATA$ROW_ID is populated with UUIDs.
func TestStream_MetadataRowID(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "ROWID_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{{Name: "ID", Type: "INTEGER"}, {Name: "VAL", Type: "VARCHAR"}}
	_, _ = repo.CreateTable(ctx, schema.ID, "SRC", columns, "")

	// Insert data, then create stream with SHOW_INITIAL_ROWS
	_, _ = executor.Execute(ctx, "INSERT INTO ROWID_DB.PUBLIC_SRC VALUES (1, 'a'), (2, 'b')")
	_, err := executor.Execute(ctx, `CREATE STREAM PUBLIC.ROWID_STREAM ON TABLE PUBLIC.SRC SHOW_INITIAL_ROWS = TRUE`)
	if err != nil {
		t.Fatalf("CREATE STREAM failed: %v", err)
	}

	stream, _ := repo.GetStreamByName(ctx, schema.ID, "ROWID_STREAM")

	// Verify changelog has 2 rows (initial snapshot)
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, stream.ChangelogTable)
	countResult, err := executor.Query(ctx, countSQL)
	if err != nil {
		t.Fatalf("Count query failed: %v", err)
	}
	count := countResult.Rows[0][0].(int64)
	if count != 2 {
		t.Fatalf("Expected 2 rows in changelog, got %d", count)
	}

	// Verify METADATA$ROW_ID is populated (at least non-null)
	nonNullSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "METADATA$ROW_ID" IS NOT NULL AND "METADATA$ROW_ID" != ''`, stream.ChangelogTable)
	nonNullResult, err := executor.Query(ctx, nonNullSQL)
	if err != nil {
		t.Fatalf("Non-null ROW_ID query failed: %v", err)
	}
	nonNullCount := nonNullResult.Rows[0][0].(int64)
	if nonNullCount != 2 {
		t.Errorf("Expected 2 rows with METADATA$ROW_ID populated, got %d", nonNullCount)
	}
}

// TestStream_StreamHasDataEmpty tests SYSTEM$STREAM_HAS_DATA on empty stream.
func TestStream_StreamHasDataEmpty(t *testing.T) {
	executor, repo, _ := setupStreamTestEnv(t)
	ctx := context.Background()

	db, _ := repo.CreateDatabase(ctx, "EMPTY_DB", "")
	schema, _ := repo.CreateSchema(ctx, db.ID, "PUBLIC", "")
	columns := []metadata.ColumnDef{{Name: "ID", Type: "INTEGER"}}
	_, _ = repo.CreateTable(ctx, schema.ID, "EMPTY_TABLE", columns, "")

	// Create stream without SHOW_INITIAL_ROWS and no DML
	_, err := executor.Execute(ctx, `CREATE STREAM PUBLIC.EMPTY_STREAM ON TABLE PUBLIC.EMPTY_TABLE`)
	if err != nil {
		t.Fatalf("CREATE STREAM failed: %v", err)
	}

	result, err := executor.Query(ctx, `SELECT SYSTEM$STREAM_HAS_DATA('EMPTY_STREAM') AS has_data`)
	if err != nil {
		t.Fatalf("SYSTEM$STREAM_HAS_DATA failed: %v", err)
	}
	if result.Rows[0][0] != false {
		t.Errorf("Expected has_data=false on empty stream, got %v", result.Rows[0][0])
	}
}

// TestParseDropStream tests the DROP STREAM parser.
func TestParseDropStream_Basic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *DropStreamStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "DROP STREAM my_stream",
			want: &DropStreamStmt{Name: "MY_STREAM"},
		},
		{
			name: "if_exists_with_schema",
			sql:  `DROP STREAM IF EXISTS "PUBLIC"."CDC_STREAM"`,
			want: &DropStreamStmt{Schema: "PUBLIC", Name: "CDC_STREAM", IfExists: true},
		},
		{
			name:    "invalid",
			sql:     "DROP TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDropStream(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDropStream() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Name != tt.want.Name {
					t.Errorf("Name = %s, want %s", got.Name, tt.want.Name)
				}
				if got.IfExists != tt.want.IfExists {
					t.Errorf("IfExists = %v, want %v", got.IfExists, tt.want.IfExists)
				}
			}
		})
	}
}
