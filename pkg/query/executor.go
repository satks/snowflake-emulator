package query

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
)

// Binding validation regexes to prevent SQL injection
var (
	// Date format: YYYY-MM-DD
	dateRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	// Time format: HH:MM:SS or HH:MM:SS.fraction
	timeRegex = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	// Timestamp format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS with optional timezone
	timestampRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:?\d{2}|Z)?$`)
)

// Executor executes SQL queries against DuckDB with Snowflake SQL translation.
type Executor struct {
	mgr             *connection.Manager
	repo            *metadata.Repository
	translator      *Translator
	copyProcessor   *CopyProcessor
	mergeProcessor  *MergeProcessor
	streamProcessor *StreamProcessor
	catalogMode     bool
}

// ExecutorOption configures an Executor.
type ExecutorOption func(*Executor)

// WithCopyProcessor sets the COPY processor for executing COPY INTO statements.
func WithCopyProcessor(processor *CopyProcessor) ExecutorOption {
	return func(e *Executor) {
		e.copyProcessor = processor
	}
}

// WithMergeProcessor sets the MERGE processor for executing MERGE INTO statements.
func WithMergeProcessor(processor *MergeProcessor) ExecutorOption {
	return func(e *Executor) {
		e.mergeProcessor = processor
	}
}

// WithStreamProcessor sets the Stream processor for executing CREATE/DROP STREAM.
func WithStreamProcessor(processor *StreamProcessor) ExecutorOption {
	return func(e *Executor) {
		e.streamProcessor = processor
	}
}

// WithCatalogMode enables catalog mode (ATTACH-based database management).
func WithCatalogMode(enabled bool) ExecutorOption {
	return func(e *Executor) {
		e.catalogMode = enabled
	}
}

// IsCatalogMode returns whether the executor is in catalog mode.
func (e *Executor) IsCatalogMode() bool {
	return e.catalogMode
}

// NewExecutor creates a new query executor.
func NewExecutor(mgr *connection.Manager, repo *metadata.Repository, opts ...ExecutorOption) *Executor {
	e := &Executor{
		mgr:        mgr,
		repo:       repo,
		translator: NewTranslator(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Configure applies options to an existing Executor.
// Use this to resolve circular dependencies when processors need the executor reference.
func (e *Executor) Configure(opts ...ExecutorOption) {
	for _, opt := range opts {
		opt(e)
	}
}

// Query executes a SELECT query and returns results.
func (e *Executor) Query(ctx context.Context, sql string) (*Result, error) {
	// Intercept SHOW/DESCRIBE commands — answer from metadata, not DuckDB
	classifier := NewClassifier()
	if classifier.IsShowSchemas(sql) {
		return e.queryShowSchemas(ctx, sql)
	}
	if classifier.IsShowTables(sql) {
		return e.queryShowTables(ctx, sql)
	}
	if classifier.IsDescribeTable(sql) {
		return e.queryDescribeTable(ctx, sql)
	}

	// Intercept SYSTEM$STREAM_HAS_DATA() function
	if strings.Contains(strings.ToUpper(sql), "SYSTEM$STREAM_HAS_DATA") {
		return e.queryStreamHasData(ctx, sql)
	}

	// Intercept INFORMATION_SCHEMA queries — DuckDB doesn't expose this for attached catalogs
	upperSQL := strings.ToUpper(sql)
	if strings.Contains(upperSQL, "INFORMATION_SCHEMA.TABLES") {
		return e.queryInformationSchemaTables(ctx, sql)
	}

	// Translate Snowflake SQL to DuckDB SQL
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	// Execute query
	rows, err := e.mgr.Query(ctx, translatedSQL)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Capture column types before iterating (using TypeMapper)
	columnTypes := InferColumnMetadata(columns, rows)

	// Fetch all rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to appropriate types
		row := make([]interface{}, len(columns))
		for i, val := range values {
			row[i] = convertValue(val)
		}

		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &Result{
		Columns:     columns,
		ColumnTypes: columnTypes,
		Rows:        resultRows,
	}, nil
}

// QueryWithBindings executes a SELECT query with parameter bindings and returns results.
// Bindings are keyed by position (e.g., "1", "2", "3") and replace :1, :2, :3 placeholders.
func (e *Executor) QueryWithBindings(ctx context.Context, sql string, bindings map[string]*QueryBindingValue) (*Result, error) {
	if len(bindings) == 0 {
		return e.Query(ctx, sql)
	}

	// Replace binding placeholders with actual values
	boundSQL, err := e.applyBindings(sql, bindings)
	if err != nil {
		return nil, fmt.Errorf("binding error: %w", err)
	}

	return e.Query(ctx, boundSQL)
}

// applyBindings replaces :N placeholders with actual values from bindings.
// Snowflake uses :1, :2, etc. for positional parameters.
func (e *Executor) applyBindings(sql string, bindings map[string]*QueryBindingValue) (string, error) {
	// Get binding keys sorted in descending order to avoid :1 replacing :10, :11, etc.
	keys := make([]int, 0, len(bindings))
	for k := range bindings {
		pos, err := strconv.Atoi(k)
		if err != nil {
			return "", fmt.Errorf("invalid binding key %q: must be a number", k)
		}
		keys = append(keys, pos)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keys)))

	result := sql
	for _, pos := range keys {
		key := strconv.Itoa(pos)
		binding := bindings[key]
		if binding == nil {
			continue
		}

		placeholder := ":" + key
		value, err := formatBindingValue(binding)
		if err != nil {
			return "", fmt.Errorf("error formatting binding %s: %w", key, err)
		}

		result = strings.ReplaceAll(result, placeholder, value)
	}

	// Also handle ? placeholders (positional, 1-based)
	result = e.replaceQuestionMarkPlaceholders(result, bindings)

	return result, nil
}

// replaceQuestionMarkPlaceholders replaces ? placeholders with binding values.
func (e *Executor) replaceQuestionMarkPlaceholders(sql string, bindings map[string]*QueryBindingValue) string {
	// Find all ? placeholders
	re := regexp.MustCompile(`\?`)
	matches := re.FindAllStringIndex(sql, -1)
	if len(matches) == 0 {
		return sql
	}

	// Replace from end to start to preserve indices
	result := sql
	for i := len(matches) - 1; i >= 0; i-- {
		key := strconv.Itoa(i + 1) // 1-based
		binding := bindings[key]
		if binding == nil {
			continue
		}

		value, err := formatBindingValue(binding)
		if err != nil {
			continue // Skip on error
		}

		start := matches[i][0]
		end := matches[i][1]
		result = result[:start] + value + result[end:]
	}

	return result
}

// formatBindingValue formats a binding value for SQL substitution.
//
//nolint:gocyclo // switch statement for type handling inherently has many branches
func formatBindingValue(b *QueryBindingValue) (string, error) {
	if b == nil {
		return ValueNull, nil
	}

	switch strings.ToUpper(b.Type) {
	case TypeText, "VARCHAR", "STRING":
		// Escape single quotes and wrap in quotes
		escaped := strings.ReplaceAll(b.Value, "'", "''")
		return "'" + escaped + "'", nil

	case "FIXED", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		// Validate it's a number
		if _, err := strconv.ParseInt(b.Value, 10, 64); err != nil {
			return "", fmt.Errorf("invalid integer value: %s", b.Value)
		}
		return b.Value, nil

	case "REAL", "FLOAT", "DOUBLE", "NUMBER", "DECIMAL":
		// Validate it's a number
		if _, err := strconv.ParseFloat(b.Value, 64); err != nil {
			return "", fmt.Errorf("invalid float value: %s", b.Value)
		}
		return b.Value, nil

	case "BOOLEAN":
		lower := strings.ToLower(b.Value)
		if lower == "true" || lower == "1" {
			return "TRUE", nil
		}
		return "FALSE", nil

	case "DATE":
		// Validate date format to prevent SQL injection
		if !dateRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid DATE format: %s (expected YYYY-MM-DD)", b.Value)
		}
		return "DATE '" + b.Value + "'", nil

	case "TIME":
		// Validate time format to prevent SQL injection
		if !timeRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid TIME format: %s (expected HH:MM:SS)", b.Value)
		}
		return "TIME '" + b.Value + "'", nil

	case "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		// Validate timestamp format to prevent SQL injection
		if !timestampRegex.MatchString(b.Value) {
			return "", fmt.Errorf("invalid TIMESTAMP format: %s (expected YYYY-MM-DD HH:MM:SS)", b.Value)
		}
		return "TIMESTAMP '" + b.Value + "'", nil

	case ValueNull:
		return ValueNull, nil

	default:
		// Default to text treatment
		escaped := strings.ReplaceAll(b.Value, "'", "''")
		return "'" + escaped + "'", nil
	}
}

// ExecuteWithBindings executes a non-query SQL statement with parameter bindings.
// Bindings are keyed by position (e.g., "1", "2", "3") and replace :1, :2, :3 placeholders.
func (e *Executor) ExecuteWithBindings(ctx context.Context, sql string, bindings map[string]*QueryBindingValue) (*ExecResult, error) {
	if len(bindings) == 0 {
		return e.Execute(ctx, sql)
	}

	// Replace binding placeholders with actual values
	boundSQL, err := e.applyBindings(sql, bindings)
	if err != nil {
		return nil, fmt.Errorf("binding error: %w", err)
	}

	return e.Execute(ctx, boundSQL)
}

// Execute executes a non-query SQL statement (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
func (e *Executor) Execute(ctx context.Context, sql string) (*ExecResult, error) {
	// Use classifier to detect DDL statements that need metadata tracking
	classifier := NewClassifier()

	// ALTER TABLE ... CLUSTER BY is a no-op (DuckDB doesn't support clustering)
	if classifier.IsAlterTableClusterBy(sql) {
		return &ExecResult{RowsAffected: 0}, nil
	}

	// Catalog mode: intercept database/schema/table DDL and USE statements
	if e.catalogMode {
		if classifier.IsCreateDatabase(sql) {
			return e.executeCreateDatabase(ctx, sql)
		}
		if classifier.IsDropDatabase(sql) {
			return e.executeDropDatabase(ctx, sql)
		}
		if classifier.IsCreateSchema(sql) {
			return e.executeCreateSchemaCatalog(ctx, sql)
		}
		if classifier.IsDropSchema(sql) {
			return e.executeDropSchemaCatalog(ctx, sql)
		}
		if classifier.IsUseDatabase(sql) {
			return e.executeUseDatabase(ctx, sql)
		}
		if classifier.IsUseSchema(sql) {
			return e.executeUseSchema(ctx, sql)
		}
		if classifier.IsCreateTable(sql) {
			return e.executeCreateTableCatalog(ctx, sql)
		}
		if classifier.IsDropTable(sql) {
			return e.executeDropTableCatalog(ctx, sql)
		}
	}

	// For CREATE TABLE, we need to register it in metadata
	if classifier.IsCreateTable(sql) {
		return e.executeCreateTable(ctx, sql)
	}

	// For DROP TABLE, we need to remove it from metadata
	if classifier.IsDropTable(sql) {
		return e.executeDropTable(ctx, sql)
	}

	// Handle transaction control statements
	if IsTransaction(sql) {
		return e.executeTransaction(ctx, sql)
	}

	// Handle COPY INTO statements
	if IsCopy(sql) {
		return e.executeCopy(ctx, sql)
	}

	// Handle MERGE INTO statements
	if IsMerge(sql) {
		return e.executeMerge(ctx, sql)
	}

	// Handle CREATE/DROP STREAM statements
	if IsCreateStream(sql) {
		return e.executeCreateStream(ctx, sql)
	}
	if IsDropStream(sql) {
		return e.executeDropStream(ctx, sql)
	}

	// Execute regular SQL statement
	return e.executeRaw(ctx, sql)
}

// executeRaw executes a SQL statement without classification or processor delegation.
// Use this from processors (COPY, MERGE) to avoid infinite recursion.
// This is a private method as it's only called from same-package processors.
func (e *Executor) executeRaw(ctx context.Context, sql string) (*ExecResult, error) {
	// Translate Snowflake SQL to DuckDB SQL
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	// Execute statement
	result, err := e.mgr.Exec(ctx, translatedSQL)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Stream CDC: record DML changes to tracked tables' changelogs
	if e.streamProcessor != nil && rowsAffected > 0 {
		e.recordStreamDMLChange(ctx, sql)
	}

	return &ExecResult{
		RowsAffected: rowsAffected,
	}, nil
}

// recordStreamDMLChange detects DML type and target table, then records to stream changelogs.
func (e *Executor) recordStreamDMLChange(ctx context.Context, sql string) {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	var action string
	var isUpdate bool
	var tableName string

	switch {
	case strings.HasPrefix(upperSQL, "INSERT"):
		action = "INSERT"
		tableName = extractTableFromInsert(upperSQL)
	case strings.HasPrefix(upperSQL, "UPDATE"):
		action = "INSERT" // UPDATE decomposes to DELETE+INSERT
		isUpdate = true
		tableName = extractTableFromUpdate(upperSQL)
	case strings.HasPrefix(upperSQL, "DELETE"):
		action = "DELETE"
		tableName = extractTableFromDelete(upperSQL)
	default:
		return
	}

	if tableName == "" {
		return
	}

	// Extract just the table name (last part of qualified name)
	parts := strings.Split(tableName, ".")
	bareTable := strings.Trim(parts[len(parts)-1], `"`)
	// Handle flat naming: SCHEMA_TABLE → TABLE part after last underscore
	if len(parts) == 2 {
		schemaPart := parts[1]
		if idx := strings.Index(schemaPart, "_"); idx > 0 {
			bareTable = schemaPart[idx+1:]
		}
	}

	e.streamProcessor.RecordDMLChange(ctx, bareTable, action, isUpdate)
}

// extractTableFromInsert extracts table name from INSERT INTO table ...
func extractTableFromInsert(upperSQL string) string {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([^\s(]+)`)
	matches := re.FindStringSubmatch(upperSQL)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// extractTableFromUpdate extracts table name from UPDATE table SET ...
func extractTableFromUpdate(upperSQL string) string {
	re := regexp.MustCompile(`(?i)UPDATE\s+([^\s]+)\s+SET`)
	matches := re.FindStringSubmatch(upperSQL)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// extractTableFromDelete extracts table name from DELETE FROM table ...
func extractTableFromDelete(upperSQL string) string {
	re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+([^\s]+)`)
	matches := re.FindStringSubmatch(upperSQL)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// executeCreateTable handles CREATE TABLE statements with metadata registration.
func (e *Executor) executeCreateTable(ctx context.Context, sql string) (*ExecResult, error) {
	// Execute the CREATE TABLE in DuckDB first
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("create table execution error: %w", err)
	}

	// Note: In a full implementation, we would parse the CREATE TABLE statement
	// and register it in metadata. For now, we just execute it.
	// This would require SQL parsing to extract table name, columns, etc.

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// executeDropTable handles DROP TABLE statements with metadata cleanup.
func (e *Executor) executeDropTable(ctx context.Context, sql string) (*ExecResult, error) {
	// Execute the DROP TABLE in DuckDB first
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("drop table execution error: %w", err)
	}

	// Note: In a full implementation, we would remove the table from metadata.
	// This would require SQL parsing to extract the table name.

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// executeTransaction handles transaction control statements (BEGIN, COMMIT, ROLLBACK).
// DuckDB supports transactions, so we pass them through directly.
func (e *Executor) executeTransaction(ctx context.Context, sql string) (*ExecResult, error) {
	// DuckDB supports BEGIN, COMMIT, and ROLLBACK
	// We execute them directly without translation
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Normalize transaction statements for DuckDB
	var duckDBSQL string
	switch {
	case strings.HasPrefix(upperSQL, "BEGIN") || strings.HasPrefix(upperSQL, "START TRANSACTION"):
		duckDBSQL = "BEGIN TRANSACTION"
	case strings.HasPrefix(upperSQL, "COMMIT"):
		duckDBSQL = "COMMIT"
	case strings.HasPrefix(upperSQL, "ROLLBACK"):
		duckDBSQL = "ROLLBACK"
	default:
		return nil, fmt.Errorf("unknown transaction statement: %s", sql)
	}

	if _, err := e.mgr.Exec(ctx, duckDBSQL); err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	return &ExecResult{
		RowsAffected: 0,
	}, nil
}

// executeCopy handles COPY INTO statements.
func (e *Executor) executeCopy(ctx context.Context, sql string) (*ExecResult, error) {
	if e.copyProcessor == nil {
		return nil, fmt.Errorf("COPY processor not configured")
	}

	// Parse the COPY statement
	stmt, err := e.copyProcessor.ParseCopyStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse COPY statement: %w", err)
	}

	// Resolve schema ID from target database/schema names if provided
	var schemaID string
	if stmt.TargetDatabase != "" && stmt.TargetSchema != "" {
		// Look up database by name
		db, err := e.repo.GetDatabaseByName(ctx, stmt.TargetDatabase)
		if err != nil {
			return nil, fmt.Errorf("database %s not found: %w", stmt.TargetDatabase, err)
		}
		// Look up schema by name
		schema, err := e.repo.GetSchemaByName(ctx, db.ID, stmt.TargetSchema)
		if err != nil {
			return nil, fmt.Errorf("schema %s not found in database %s: %w", stmt.TargetSchema, stmt.TargetDatabase, err)
		}
		schemaID = schema.ID
	}

	// Execute COPY INTO with resolved schema context
	result, err := e.copyProcessor.ExecuteCopyInto(ctx, stmt, schemaID)
	if err != nil {
		return nil, fmt.Errorf("COPY INTO failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsLoaded,
	}, nil
}

// executeMerge handles MERGE INTO statements.
func (e *Executor) executeMerge(ctx context.Context, sql string) (*ExecResult, error) {
	if e.mergeProcessor == nil {
		return nil, fmt.Errorf("MERGE processor not configured")
	}

	// Parse the MERGE statement
	stmt, err := e.mergeProcessor.ParseMergeStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MERGE statement: %w", err)
	}

	// Execute MERGE
	result, err := e.mergeProcessor.ExecuteMerge(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("MERGE failed: %w", err)
	}

	return &ExecResult{
		RowsAffected: result.RowsInserted + result.RowsUpdated + result.RowsDeleted,
	}, nil
}

// convertValue converts database values to appropriate Go types.
func convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		// Convert byte slices to strings
		return string(v)
	case int64:
		// Keep as int64 for now, could convert to int if needed
		return v
	case float64:
		return v
	case bool:
		return v
	case string:
		return v
	default:
		// For other types, return as-is
		return v
	}
}

// SHOW/DESCRIBE query methods — build Result from metadata repository

// buildColumnTypes creates ColumnMetadata for hand-constructed result sets (SHOW/DESCRIBE)
// where we don't have sql.Rows to infer types from.
func buildColumnTypes(columns []string) []types.ColumnMetadata {
	result := make([]types.ColumnMetadata, len(columns))
	for i, col := range columns {
		result[i] = types.ColumnMetadata{
			Name:     col,
			Type:     "TEXT",
			Nullable: true,
		}
	}
	return result
}

// queryShowSchemas handles SHOW SCHEMAS [IN DATABASE name].
func (e *Executor) queryShowSchemas(ctx context.Context, sql string) (*Result, error) {
	stmt, err := ParseShowSchemas(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW SCHEMAS: %w", err)
	}

	columns := []string{"created_on", "name", "is_default", "is_current", "database_name", "owner", "comment", "options", "retention_time", "owner_role_type"}
	var rows [][]interface{}

	buildRow := func(schemaName, dbName, createdAt string) []interface{} {
		return []interface{}{createdAt, schemaName, "N", "N", dbName, "", "", "", "1", ""}
	}

	if stmt.Database != "" {
		// Show schemas in specific database
		db, err := e.repo.GetDatabaseByName(ctx, stmt.Database)
		if err != nil {
			return nil, fmt.Errorf("database %s not found: %w", stmt.Database, err)
		}
		schemas, err := e.repo.ListSchemas(ctx, db.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to list schemas: %w", err)
		}
		for _, s := range schemas {
			rows = append(rows, buildRow(s.Name, db.Name, s.CreatedAt.Format(time.RFC3339)))
		}
	} else {
		// Show schemas across all databases
		databases, err := e.repo.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}
		for _, db := range databases {
			schemas, err := e.repo.ListSchemas(ctx, db.ID)
			if err != nil {
				continue
			}
			for _, s := range schemas {
				rows = append(rows, buildRow(s.Name, db.Name, s.CreatedAt.Format(time.RFC3339)))
			}
		}
	}

	return &Result{Columns: columns, ColumnTypes: buildColumnTypes(columns), Rows: rows}, nil
}

// queryShowTables handles SHOW TABLES [IN [db.]schema].
func (e *Executor) queryShowTables(ctx context.Context, sql string) (*Result, error) {
	stmt, err := ParseShowTables(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW TABLES: %w", err)
	}

	columns := []string{"created_on", "name", "database_name", "schema_name", "kind", "comment", "cluster_by", "rows", "bytes", "owner", "retention_time"}
	var rows [][]interface{}

	buildRow := func(tableName, dbName, schemaName, kind, createdAt string) []interface{} {
		if kind == "" {
			kind = "TABLE"
		}
		return []interface{}{createdAt, tableName, dbName, schemaName, kind, "", "", int64(0), int64(0), "", "1"}
	}

	if stmt.Database != "" && stmt.Schema != "" {
		// Fully qualified: db.schema
		db, err := e.repo.GetDatabaseByName(ctx, stmt.Database)
		if err != nil {
			return nil, fmt.Errorf("database %s not found: %w", stmt.Database, err)
		}
		schema, err := e.repo.GetSchemaByName(ctx, db.ID, stmt.Schema)
		if err != nil {
			return nil, fmt.Errorf("schema %s not found: %w", stmt.Schema, err)
		}
		tables, err := e.repo.ListTables(ctx, schema.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		for _, t := range tables {
			rows = append(rows, buildRow(t.Name, db.Name, schema.Name, t.TableType, t.CreatedAt.Format(time.RFC3339)))
		}
	} else if stmt.Schema != "" {
		// Schema only: find first matching schema across databases
		databases, err := e.repo.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}
		for _, db := range databases {
			schema, err := e.repo.GetSchemaByName(ctx, db.ID, stmt.Schema)
			if err != nil {
				continue
			}
			tables, err := e.repo.ListTables(ctx, schema.ID)
			if err != nil {
				continue
			}
			for _, t := range tables {
				rows = append(rows, buildRow(t.Name, db.Name, schema.Name, t.TableType, t.CreatedAt.Format(time.RFC3339)))
			}
		}
	} else {
		// Bare SHOW TABLES: list all tables across all databases/schemas
		databases, err := e.repo.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}
		for _, db := range databases {
			schemas, err := e.repo.ListSchemas(ctx, db.ID)
			if err != nil {
				continue
			}
			for _, s := range schemas {
				tables, err := e.repo.ListTables(ctx, s.ID)
				if err != nil {
					continue
				}
				for _, t := range tables {
					rows = append(rows, buildRow(t.Name, db.Name, s.Name, t.TableType, t.CreatedAt.Format(time.RFC3339)))
				}
			}
		}
	}

	return &Result{Columns: columns, ColumnTypes: buildColumnTypes(columns), Rows: rows}, nil
}

// queryDescribeTable handles DESCRIBE TABLE [db.][schema.]table.
// If metadata lookup fails (e.g., table created via raw SQL without metadata registration),
// falls back to DuckDB's native DESCRIBE.
func (e *Executor) queryDescribeTable(ctx context.Context, sql string) (*Result, error) {
	stmt, err := ParseDescribeTable(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DESCRIBE TABLE: %w", err)
	}

	// Try metadata-based describe first
	result, metaErr := e.describeTableFromMetadata(ctx, stmt)
	if metaErr == nil && len(result.Rows) > 0 {
		return result, nil
	}

	// Fallback: use DuckDB native DESCRIBE and remap columns to Snowflake format
	tableName := stmt.Table
	if stmt.Schema != "" {
		tableName = stmt.Schema + "." + tableName
	}
	if stmt.Database != "" {
		tableName = stmt.Database + "." + tableName
	}

	fallbackSQL := "DESCRIBE " + tableName
	rows, err := e.mgr.Query(ctx, fallbackSQL)
	if err != nil {
		// If fallback also fails, return the original metadata error for better context
		if metaErr != nil {
			return nil, fmt.Errorf("DESCRIBE TABLE failed: %w", metaErr)
		}
		return nil, fmt.Errorf("DESCRIBE TABLE failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	duckColumns, _ := rows.Columns()

	// Map DuckDB column indices by name for remapping
	duckColIndex := make(map[string]int)
	for i, c := range duckColumns {
		duckColIndex[strings.ToLower(c)] = i
	}

	var duckRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(duckColumns))
		valuePtrs := make([]interface{}, len(duckColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan DESCRIBE row: %w", err)
		}
		row := make([]interface{}, len(duckColumns))
		for i, val := range values {
			row[i] = convertValue(val)
		}
		duckRows = append(duckRows, row)
	}

	// Remap DuckDB columns to Snowflake DESCRIBE TABLE format
	sfColumns := []string{"name", "type", "kind", "null?", "default", "primary key", "unique key", "check", "expression", "comment", "policy name", "privacy domain"}
	// DuckDB DESCRIBE returns: column_name, column_type, null, key, default, extra
	duckToSF := map[string]string{
		"column_name": "name",
		"column_type": "type",
		"null":        "null?",
		"key":         "primary key",
		"default":     "default",
		"extra":       "comment",
	}

	var sfRows [][]interface{}
	for _, duckRow := range duckRows {
		sfRow := make([]interface{}, len(sfColumns))
		// Initialize with empty defaults
		for i := range sfRow {
			sfRow[i] = ""
		}
		sfRow[2] = "COLUMN" // kind

		for duckName, sfName := range duckToSF {
			dIdx, ok := duckColIndex[duckName]
			if !ok {
				continue
			}
			// Find sfName index in sfColumns
			for sIdx, sc := range sfColumns {
				if sc == sfName {
					val := duckRow[dIdx]
					// Normalize null? column: DuckDB returns "YES"/"NO", Snowflake expects "Y"/"N"
					if sfName == "null?" {
						switch fmt.Sprintf("%v", val) {
						case "YES":
							val = "Y"
						case "NO":
							val = "N"
						}
					}
					// Normalize primary key: DuckDB returns "PRI" or empty
					if sfName == "primary key" {
						if fmt.Sprintf("%v", val) == "PRI" {
							val = "Y"
						} else {
							val = "N"
						}
					}
					sfRow[sIdx] = val
					break
				}
			}
		}
		sfRows = append(sfRows, sfRow)
	}

	return &Result{Columns: sfColumns, ColumnTypes: buildColumnTypes(sfColumns), Rows: sfRows}, nil
}

// describeTableFromMetadata attempts to describe a table using the metadata repository.
func (e *Executor) describeTableFromMetadata(ctx context.Context, stmt *DescribeTableStmt) (*Result, error) {
	var db *metadata.Database
	var err error

	if stmt.Database != "" {
		db, err = e.repo.GetDatabaseByName(ctx, stmt.Database)
		if err != nil {
			return nil, err
		}
	} else {
		databases, err := e.repo.ListDatabases(ctx)
		if err != nil || len(databases) == 0 {
			return nil, fmt.Errorf("no databases found")
		}
		db = databases[0]
	}

	var schema *metadata.Schema
	if stmt.Schema != "" {
		schema, err = e.repo.GetSchemaByName(ctx, db.ID, stmt.Schema)
		if err != nil {
			return nil, err
		}
	} else {
		schemas, err := e.repo.ListSchemas(ctx, db.ID)
		if err != nil || len(schemas) == 0 {
			return nil, fmt.Errorf("no schemas found in database %s", db.Name)
		}
		schema = schemas[0]
	}

	table, err := e.repo.GetTableByName(ctx, schema.ID, stmt.Table)
	if err != nil {
		return nil, err
	}

	columns := []string{"name", "type", "kind", "null?", "default", "primary key", "unique key", "check", "expression", "comment", "policy name", "privacy domain"}
	var rows [][]interface{}

	if table.ColumnDefinitions != "" {
		colParts := strings.Split(table.ColumnDefinitions, ";")
		for _, part := range colParts {
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 5)
			if len(fields) < 4 {
				continue
			}

			name := fields[0]
			colType := fields[1]
			nullable := fields[2]
			pk := fields[3]
			defaultVal := ""
			if len(fields) == 5 {
				defaultVal = fields[4]
			}

			nullDisplay := "Y"
			if nullable == "false" {
				nullDisplay = "N"
			}
			pkDisplay := "N"
			if pk == "true" {
				pkDisplay = "Y"
			}

			rows = append(rows, []interface{}{
				name,
				colType,
				"COLUMN",
				nullDisplay,
				defaultVal,
				pkDisplay,
				"N",  // unique key
				"",   // check
				"",   // expression
				"",   // comment
				"",   // policy name
				"",   // privacy domain
			})
		}
	}

	return &Result{Columns: columns, ColumnTypes: buildColumnTypes(columns), Rows: rows}, nil
}

// queryInformationSchemaTables handles SELECT ... FROM INFORMATION_SCHEMA.TABLES queries.
// DuckDB doesn't expose INFORMATION_SCHEMA for attached catalogs, so we answer from metadata.
func (e *Executor) queryInformationSchemaTables(ctx context.Context, _ string) (*Result, error) {
	columns := []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"}
	var rows [][]interface{}

	databases, err := e.repo.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	for _, db := range databases {
		schemas, err := e.repo.ListSchemas(ctx, db.ID)
		if err != nil {
			continue
		}
		for _, s := range schemas {
			tables, err := e.repo.ListTables(ctx, s.ID)
			if err != nil {
				continue
			}
			for _, t := range tables {
				tableType := "BASE TABLE"
				if t.TableType != "" {
					tableType = t.TableType
				}
				rows = append(rows, []interface{}{
					db.Name,
					s.Name,
					t.Name,
					tableType,
				})
			}
		}
	}

	return &Result{Columns: columns, ColumnTypes: buildColumnTypes(columns), Rows: rows}, nil
}

// Stream execution methods

// executeCreateStream handles CREATE STREAM statements.
func (e *Executor) executeCreateStream(ctx context.Context, sql string) (*ExecResult, error) {
	if e.streamProcessor == nil {
		return nil, fmt.Errorf("stream processor not configured")
	}

	stmt, err := ParseCreateStream(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE STREAM: %w", err)
	}

	// Resolve schema for the stream
	schemaID, err := e.resolveSchemaID(ctx, stmt.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema: %w", err)
	}

	return e.streamProcessor.ExecuteCreateStream(ctx, stmt, schemaID)
}

// executeDropStream handles DROP STREAM statements.
func (e *Executor) executeDropStream(ctx context.Context, sql string) (*ExecResult, error) {
	if e.streamProcessor == nil {
		return nil, fmt.Errorf("stream processor not configured")
	}

	stmt, err := ParseDropStream(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DROP STREAM: %w", err)
	}

	schemaID, err := e.resolveSchemaID(ctx, stmt.Schema)
	if err != nil {
		if stmt.IfExists {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("failed to resolve schema: %w", err)
	}

	return e.streamProcessor.ExecuteDropStream(ctx, stmt, schemaID)
}

// queryStreamHasData handles SELECT SYSTEM$STREAM_HAS_DATA(...).
func (e *Executor) queryStreamHasData(ctx context.Context, sql string) (*Result, error) {
	if e.streamProcessor == nil {
		return nil, fmt.Errorf("stream processor not configured")
	}

	streamName, err := ParseStreamHasData(sql)
	if err != nil {
		return nil, err
	}

	// Try to find stream across all schemas
	databases, _ := e.repo.ListDatabases(ctx)
	for _, db := range databases {
		schemas, _ := e.repo.ListSchemas(ctx, db.ID)
		for _, schema := range schemas {
			hasData, err := e.streamProcessor.QueryStreamHasData(ctx, streamName, schema.ID)
			if err == nil {
				return &Result{
					Columns: []string{"HAS_DATA"},
					Rows:    [][]interface{}{{hasData}},
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("stream %s not found", streamName)
}

// resolveSchemaID resolves a schema name to its ID, searching all databases.
func (e *Executor) resolveSchemaID(ctx context.Context, schemaName string) (string, error) {
	if schemaName == "" {
		// Use first available schema
		databases, err := e.repo.ListDatabases(ctx)
		if err != nil || len(databases) == 0 {
			return "", fmt.Errorf("no databases found")
		}
		schemas, err := e.repo.ListSchemas(ctx, databases[0].ID)
		if err != nil || len(schemas) == 0 {
			return "", fmt.Errorf("no schemas found")
		}
		return schemas[0].ID, nil
	}

	// Search for schema by name across all databases
	databases, err := e.repo.ListDatabases(ctx)
	if err != nil {
		return "", err
	}
	for _, db := range databases {
		schema, err := e.repo.GetSchemaByName(ctx, db.ID, schemaName)
		if err == nil {
			return schema.ID, nil
		}
	}
	return "", fmt.Errorf("schema %s not found", schemaName)
}

// Catalog mode execution methods

// executeCreateDatabase handles CREATE DATABASE via catalog mode (ATTACH).
func (e *Executor) executeCreateDatabase(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseCreateDatabase(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE DATABASE: %w", err)
	}

	_, err = e.repo.CreateDatabaseCatalog(ctx, stmt.Name, "", stmt.IfNotExists)
	if err != nil {
		return nil, fmt.Errorf("CREATE DATABASE failed: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeDropDatabase handles DROP DATABASE via catalog mode (DETACH).
func (e *Executor) executeDropDatabase(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseDropDatabase(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DROP DATABASE: %w", err)
	}

	if err := e.repo.DropDatabaseCatalog(ctx, stmt.Name, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("DROP DATABASE failed: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeCreateSchemaCatalog handles CREATE SCHEMA via catalog mode.
func (e *Executor) executeCreateSchemaCatalog(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseCreateSchema(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE SCHEMA: %w", err)
	}

	if stmt.Database == "" {
		return nil, fmt.Errorf("CREATE SCHEMA requires database prefix in catalog mode (e.g., CREATE SCHEMA db.schema)")
	}

	db, err := e.repo.GetDatabaseByName(ctx, stmt.Database)
	if err != nil {
		return nil, fmt.Errorf("database %s not found: %w", stmt.Database, err)
	}

	_, err = e.repo.CreateSchemaCatalog(ctx, db.ID, stmt.Schema, "", stmt.IfNotExists)
	if err != nil {
		return nil, fmt.Errorf("CREATE SCHEMA failed: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeDropSchemaCatalog handles DROP SCHEMA via catalog mode.
func (e *Executor) executeDropSchemaCatalog(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseDropSchema(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DROP SCHEMA: %w", err)
	}

	if stmt.Database == "" {
		return nil, fmt.Errorf("DROP SCHEMA requires database prefix in catalog mode (e.g., DROP SCHEMA db.schema)")
	}

	db, err := e.repo.GetDatabaseByName(ctx, stmt.Database)
	if err != nil {
		if stmt.IfExists {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("database %s not found: %w", stmt.Database, err)
	}

	schema, err := e.repo.GetSchemaByName(ctx, db.ID, stmt.Schema)
	if err != nil {
		if stmt.IfExists {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("schema %s not found: %w", stmt.Schema, err)
	}

	if err := e.repo.DropSchemaCatalog(ctx, schema.ID, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("DROP SCHEMA failed: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeUseDatabase handles USE DATABASE by validating the database exists.
// Actual session context update happens at the handler layer.
func (e *Executor) executeUseDatabase(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseUseDatabase(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse USE DATABASE: %w", err)
	}

	_, err = e.repo.GetDatabaseByName(ctx, stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("database %s not found: %w", stmt.Name, err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeUseSchema handles USE SCHEMA by validating the schema name.
// Actual session context update happens at the handler layer.
func (e *Executor) executeUseSchema(ctx context.Context, sql string) (*ExecResult, error) {
	stmt, err := ParseUseSchema(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse USE SCHEMA: %w", err)
	}

	// We validate the name is parseable but don't fail if schema doesn't exist yet,
	// since we don't know the current database context at the executor level.
	_ = stmt

	return &ExecResult{RowsAffected: 0}, nil
}

// executeCreateTableCatalog handles CREATE TABLE in catalog mode.
// Passes the SQL through to DuckDB directly (three-part names resolve natively).
func (e *Executor) executeCreateTableCatalog(ctx context.Context, sql string) (*ExecResult, error) {
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("create table execution error: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// executeDropTableCatalog handles DROP TABLE in catalog mode.
// Passes the SQL through to DuckDB directly (three-part names resolve natively).
func (e *Executor) executeDropTableCatalog(ctx context.Context, sql string) (*ExecResult, error) {
	translatedSQL, err := e.translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	if _, err := e.mgr.Exec(ctx, translatedSQL); err != nil {
		return nil, fmt.Errorf("drop table execution error: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// ExecuteWithHistory wraps Execute with query history tracking.
func (e *Executor) ExecuteWithHistory(ctx context.Context, sessionID, queryID, sql string) (*ExecResult, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute the query
	result, execErr := e.Execute(ctx, sql)

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Record result
	if entry != nil {
		if execErr != nil {
			_ = e.repo.RecordQueryFailure(ctx, entry.ID, execErr.Error(), executionTimeMs)
		} else {
			_ = e.repo.RecordQuerySuccess(ctx, entry.ID, result.RowsAffected, executionTimeMs)
		}
	}

	return result, execErr
}

// QueryWithHistory wraps Query with query history tracking.
func (e *Executor) QueryWithHistory(ctx context.Context, sessionID, queryID, sql string) (*Result, error) {
	startTime := time.Now()

	// Record query start (non-blocking on failure)
	entry, err := e.repo.RecordQueryStart(ctx, sessionID, queryID, sql)
	if err != nil {
		log.Printf("Failed to record query start: %v", err)
	}

	// Execute the query
	result, execErr := e.Query(ctx, sql)

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Record result
	if entry != nil {
		if execErr != nil {
			_ = e.repo.RecordQueryFailure(ctx, entry.ID, execErr.Error(), executionTimeMs)
		} else {
			var rowCount int64
			if result != nil {
				rowCount = int64(len(result.Rows))
			}
			_ = e.repo.RecordQuerySuccess(ctx, entry.ID, rowCount, executionTimeMs)
		}
	}

	return result, execErr
}
