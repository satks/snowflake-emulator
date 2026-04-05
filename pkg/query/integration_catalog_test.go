package query

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// TestCatalogIntegration_QueryEngineWorkflow tests the complete query engine workflow in catalog mode.
func TestCatalogIntegration_QueryEngineWorkflow(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo, WithCatalogMode(true))
	ctx := context.Background()

	// Create database and schema via catalog mode
	database, err := repo.CreateDatabaseCatalog(ctx, "ANALYTICS_DB", "Analytics database", false)
	if err != nil {
		t.Fatalf("CreateDatabaseCatalog() error = %v", err)
	}

	schema, err := repo.CreateSchemaCatalog(ctx, database.ID, "PROD", "Production schema", false)
	if err != nil {
		t.Fatalf("CreateSchemaCatalog() error = %v", err)
	}

	// Create tables using catalog mode (three-part names)
	customerCols := []metadata.ColumnDef{
		{Name: "CUSTOMER_ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "NAME", Type: "VARCHAR"},
		{Name: "EMAIL", Type: "VARCHAR", Nullable: true},
		{Name: "IS_ACTIVE", Type: "BOOLEAN"},
	}
	_, err = repo.CreateTableCatalog(ctx, schema.ID, "CUSTOMERS", customerCols, "")
	if err != nil {
		t.Fatalf("CreateTableCatalog(CUSTOMERS) error = %v", err)
	}

	orderCols := []metadata.ColumnDef{
		{Name: "ORDER_ID", Type: "INTEGER", PrimaryKey: true},
		{Name: "CUSTOMER_ID", Type: "INTEGER"},
		{Name: "AMOUNT", Type: "DOUBLE"},
		{Name: "STATUS", Type: "VARCHAR"},
	}
	_, err = repo.CreateTableCatalog(ctx, schema.ID, "ORDERS", orderCols, "")
	if err != nil {
		t.Fatalf("CreateTableCatalog(ORDERS) error = %v", err)
	}

	// Insert data using three-part names
	customerInsertSQL := `
		INSERT INTO "ANALYTICS_DB"."PROD"."CUSTOMERS" (CUSTOMER_ID, NAME, EMAIL, IS_ACTIVE) VALUES
		(1, 'Alice Johnson', 'alice@example.com', true),
		(2, 'Bob Smith', 'bob@example.com', true),
		(3, 'Charlie Brown', NULL, false)
	`
	_, err = executor.Execute(ctx, customerInsertSQL)
	if err != nil {
		t.Fatalf("Insert customers error = %v", err)
	}

	orderInsertSQL := `
		INSERT INTO "ANALYTICS_DB"."PROD"."ORDERS" (ORDER_ID, CUSTOMER_ID, AMOUNT, STATUS) VALUES
		(101, 1, 150.50, 'completed'),
		(102, 1, 200.00, 'completed'),
		(103, 2, 75.25, 'pending'),
		(104, 3, 300.00, 'canceled')
	`
	_, err = executor.Execute(ctx, orderInsertSQL)
	if err != nil {
		t.Fatalf("Insert orders error = %v", err)
	}

	// Test subtests with Snowflake function translations
	t.Run("IFF_WithAggregation", func(t *testing.T) {
		sql := `SELECT
			NAME,
			IFF(IS_ACTIVE, 'Active', 'Inactive') AS status
		FROM "ANALYTICS_DB"."PROD"."CUSTOMERS"
		ORDER BY CUSTOMER_ID`

		result, err := executor.Query(ctx, sql)
		if err != nil {
			t.Fatalf("Query error = %v", err)
		}
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
		// First row should be Alice, Active
		if len(result.Rows) > 0 {
			if result.Rows[0][1] != "Active" {
				t.Errorf("Expected 'Active', got %v", result.Rows[0][1])
			}
		}
	})

	t.Run("NVL_WithJoin", func(t *testing.T) {
		sql := `SELECT
			c.NAME,
			NVL(c.EMAIL, 'no-email') AS email,
			o.AMOUNT
		FROM "ANALYTICS_DB"."PROD"."CUSTOMERS" c
		JOIN "ANALYTICS_DB"."PROD"."ORDERS" o ON c.CUSTOMER_ID = o.CUSTOMER_ID
		WHERE o.STATUS = 'completed'
		ORDER BY o.ORDER_ID`

		result, err := executor.Query(ctx, sql)
		if err != nil {
			t.Fatalf("Query error = %v", err)
		}
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows (Alice's completed orders), got %d", len(result.Rows))
		}
	})

	t.Run("CONCAT_Function", func(t *testing.T) {
		sql := `SELECT CONCAT(NAME, ' (', NVL(EMAIL, 'N/A'), ')') AS display
		FROM "ANALYTICS_DB"."PROD"."CUSTOMERS"
		WHERE CUSTOMER_ID = 3`

		result, err := executor.Query(ctx, sql)
		if err != nil {
			t.Fatalf("Query error = %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}
		expected := "Charlie Brown (N/A)"
		if result.Rows[0][0] != expected {
			t.Errorf("Expected '%s', got %v", expected, result.Rows[0][0])
		}
	})

	t.Run("UPDATE_And_DELETE", func(t *testing.T) {
		// Update
		updateSQL := `UPDATE "ANALYTICS_DB"."PROD"."ORDERS" SET STATUS = 'shipped' WHERE ORDER_ID = 103`
		updateResult, err := executor.Execute(ctx, updateSQL)
		if err != nil {
			t.Fatalf("UPDATE error = %v", err)
		}
		if updateResult.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", updateResult.RowsAffected)
		}

		// Delete
		deleteSQL := `DELETE FROM "ANALYTICS_DB"."PROD"."ORDERS" WHERE STATUS = 'canceled'`
		deleteResult, err := executor.Execute(ctx, deleteSQL)
		if err != nil {
			t.Fatalf("DELETE error = %v", err)
		}
		if deleteResult.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", deleteResult.RowsAffected)
		}

		// Verify
		result, err := executor.Query(ctx, `SELECT COUNT(*) FROM "ANALYTICS_DB"."PROD"."ORDERS"`)
		if err != nil {
			t.Fatalf("SELECT COUNT error = %v", err)
		}
		if result.Rows[0][0] != int64(3) {
			t.Errorf("Expected 3 remaining orders, got %v", result.Rows[0][0])
		}
	})
}

// TestCatalogIntegration_AllSQLOperations tests all SQL operations in catalog mode.
func TestCatalogIntegration_AllSQLOperations(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := connection.NewManager(db)
	repo, err := metadata.NewRepository(mgr)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	executor := NewExecutor(mgr, repo, WithCatalogMode(true))
	mergeProcessor := NewMergeProcessor(executor)
	executor.Configure(WithMergeProcessor(mergeProcessor))
	ctx := context.Background()

	t.Run("DDL_Database_Schema_SQL", func(t *testing.T) {
		// CREATE DATABASE via SQL
		_, err := executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS SQL_OPS_DB")
		if err != nil {
			t.Fatalf("CREATE DATABASE error = %v", err)
		}

		// CREATE SCHEMA via SQL
		_, err = executor.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS SQL_OPS_DB.OPS_SCHEMA")
		if err != nil {
			t.Fatalf("CREATE SCHEMA error = %v", err)
		}

		// CREATE TABLE via SQL
		_, err = executor.Execute(ctx, `CREATE TABLE "SQL_OPS_DB"."OPS_SCHEMA"."DDL_TEST" (id INTEGER, name VARCHAR)`)
		if err != nil {
			t.Fatalf("CREATE TABLE error = %v", err)
		}

		// INSERT
		_, err = executor.Execute(ctx, `INSERT INTO "SQL_OPS_DB"."OPS_SCHEMA"."DDL_TEST" VALUES (1, 'test')`)
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}

		// SELECT
		result, err := executor.Query(ctx, `SELECT * FROM "SQL_OPS_DB"."OPS_SCHEMA"."DDL_TEST"`)
		if err != nil {
			t.Fatalf("SELECT error = %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}

		// DROP TABLE
		_, err = executor.Execute(ctx, `DROP TABLE IF EXISTS "SQL_OPS_DB"."OPS_SCHEMA"."DDL_TEST"`)
		if err != nil {
			t.Fatalf("DROP TABLE error = %v", err)
		}

		// DROP SCHEMA
		_, err = executor.Execute(ctx, "DROP SCHEMA IF EXISTS SQL_OPS_DB.OPS_SCHEMA")
		if err != nil {
			t.Fatalf("DROP SCHEMA error = %v", err)
		}

		// DROP DATABASE
		_, err = executor.Execute(ctx, "DROP DATABASE IF EXISTS SQL_OPS_DB")
		if err != nil {
			t.Fatalf("DROP DATABASE error = %v", err)
		}
	})

	t.Run("Transactions", func(t *testing.T) {
		_, _ = executor.Execute(ctx, "CREATE DATABASE IF NOT EXISTS TX_DB")
		_, _ = executor.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS TX_DB.TX_SCHEMA")
		_, err := executor.Execute(ctx, `CREATE TABLE "TX_DB"."TX_SCHEMA"."TX_TABLE" (id INTEGER, val VARCHAR)`)
		if err != nil {
			t.Fatalf("CREATE TABLE error = %v", err)
		}

		// Begin + Insert + Commit
		_, _ = executor.Execute(ctx, "BEGIN")
		_, _ = executor.Execute(ctx, `INSERT INTO "TX_DB"."TX_SCHEMA"."TX_TABLE" VALUES (1, 'committed')`)
		_, _ = executor.Execute(ctx, "COMMIT")

		result, _ := executor.Query(ctx, `SELECT val FROM "TX_DB"."TX_SCHEMA"."TX_TABLE" WHERE id = 1`)
		if len(result.Rows) != 1 || result.Rows[0][0] != "committed" {
			t.Errorf("Expected committed row, got %v", result.Rows)
		}

		// Begin + Insert + Rollback
		_, _ = executor.Execute(ctx, "BEGIN")
		_, _ = executor.Execute(ctx, `INSERT INTO "TX_DB"."TX_SCHEMA"."TX_TABLE" VALUES (2, 'rolled_back')`)
		_, _ = executor.Execute(ctx, "ROLLBACK")

		result, _ = executor.Query(ctx, `SELECT COUNT(*) FROM "TX_DB"."TX_SCHEMA"."TX_TABLE"`)
		if result.Rows[0][0] != int64(1) {
			t.Errorf("Expected 1 row after rollback, got %v", result.Rows[0][0])
		}

		// Cleanup
		_, _ = executor.Execute(ctx, "DROP DATABASE IF EXISTS TX_DB")
	})

	t.Run("ParameterBindings", func(t *testing.T) {
		result, err := executor.QueryWithBindings(ctx,
			"SELECT :1 AS a, :2 AS b",
			map[string]*QueryBindingValue{
				"1": {Type: "FIXED", Value: "42"},
				"2": {Type: "TEXT", Value: "hello"},
			},
		)
		if err != nil {
			t.Fatalf("QueryWithBindings error = %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}
	})
}
