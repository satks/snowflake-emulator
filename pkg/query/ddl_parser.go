// Package query provides SQL query execution against DuckDB with Snowflake SQL translation.
package query

import (
	"fmt"
	"regexp"
	"strings"
)

// CreateDatabaseStmt represents a parsed CREATE DATABASE statement.
type CreateDatabaseStmt struct {
	Name        string
	IfNotExists bool
}

// DropDatabaseStmt represents a parsed DROP DATABASE statement.
type DropDatabaseStmt struct {
	Name     string
	IfExists bool
}

// CreateSchemaStmt represents a parsed CREATE SCHEMA statement.
type CreateSchemaStmt struct {
	Database    string // empty if not specified
	Schema      string
	IfNotExists bool
}

// DropSchemaStmt represents a parsed DROP SCHEMA statement.
type DropSchemaStmt struct {
	Database string // empty if not specified
	Schema   string
	IfExists bool
}

// UseDatabaseStmt represents a parsed USE DATABASE statement.
type UseDatabaseStmt struct {
	Name string
}

// UseSchemaStmt represents a parsed USE SCHEMA statement.
type UseSchemaStmt struct {
	Name string
}

// Regex patterns for DDL parsing.
var (
	createDatabaseRe = regexp.MustCompile(`(?i)^\s*CREATE\s+DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	dropDatabaseRe   = regexp.MustCompile(`(?i)^\s*DROP\s+DATABASE\s+(IF\s+EXISTS\s+)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	createSchemaRe   = regexp.MustCompile(`(?i)^\s*CREATE\s+SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?(("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	dropSchemaRe     = regexp.MustCompile(`(?i)^\s*DROP\s+SCHEMA\s+(IF\s+EXISTS\s+)?(("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	useDatabaseRe    = regexp.MustCompile(`(?i)^\s*USE\s+DATABASE\s+("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	useSchemaRe      = regexp.MustCompile(`(?i)^\s*USE\s+(?:SCHEMA\s+)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
	createStreamRe   = regexp.MustCompile(`(?i)^\s*CREATE\s+STREAM\s+(IF\s+NOT\s+EXISTS\s+)?(("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s+ON\s+TABLE\s+(("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?(?:("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)(.*)$`)
	dropStreamRe     = regexp.MustCompile(`(?i)^\s*DROP\s+STREAM\s+(IF\s+EXISTS\s+)?(("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
)

// normalizeIdentifier strips quotes and uppercases unquoted identifiers.
func normalizeIdentifier(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, `"`) && strings.HasSuffix(id, `"`) {
		// Quoted identifier: strip quotes, preserve case
		return strings.Trim(id, `"`)
	}
	// Unquoted identifier: normalize to uppercase
	return strings.ToUpper(id)
}

// ParseCreateDatabase parses a CREATE DATABASE statement.
func ParseCreateDatabase(sql string) (*CreateDatabaseStmt, error) {
	matches := createDatabaseRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE DATABASE statement: %s", sql)
	}

	return &CreateDatabaseStmt{
		Name:        normalizeIdentifier(matches[2]),
		IfNotExists: matches[1] != "",
	}, nil
}

// ParseDropDatabase parses a DROP DATABASE statement.
func ParseDropDatabase(sql string) (*DropDatabaseStmt, error) {
	matches := dropDatabaseRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid DROP DATABASE statement: %s", sql)
	}

	return &DropDatabaseStmt{
		Name:     normalizeIdentifier(matches[2]),
		IfExists: matches[1] != "",
	}, nil
}

// ParseCreateSchema parses a CREATE SCHEMA statement.
// Supports: CREATE SCHEMA [IF NOT EXISTS] [database.]schema
func ParseCreateSchema(sql string) (*CreateSchemaStmt, error) {
	matches := createSchemaRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE SCHEMA statement: %s", sql)
	}

	stmt := &CreateSchemaStmt{
		IfNotExists: matches[1] != "",
		Schema:      normalizeIdentifier(matches[4]),
	}

	// matches[3] is the database part (before the dot), if present
	if matches[3] != "" {
		stmt.Database = normalizeIdentifier(matches[3])
	}

	return stmt, nil
}

// ParseDropSchema parses a DROP SCHEMA statement.
// Supports: DROP SCHEMA [IF EXISTS] [database.]schema
func ParseDropSchema(sql string) (*DropSchemaStmt, error) {
	matches := dropSchemaRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid DROP SCHEMA statement: %s", sql)
	}

	stmt := &DropSchemaStmt{
		IfExists: matches[1] != "",
		Schema:   normalizeIdentifier(matches[4]),
	}

	// matches[3] is the database part (before the dot), if present
	if matches[3] != "" {
		stmt.Database = normalizeIdentifier(matches[3])
	}

	return stmt, nil
}

// ParseUseDatabase parses a USE DATABASE statement.
func ParseUseDatabase(sql string) (*UseDatabaseStmt, error) {
	matches := useDatabaseRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid USE DATABASE statement: %s", sql)
	}

	return &UseDatabaseStmt{
		Name: normalizeIdentifier(matches[1]),
	}, nil
}

// ParseUseSchema parses a USE SCHEMA or bare USE statement.
func ParseUseSchema(sql string) (*UseSchemaStmt, error) {
	matches := useSchemaRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid USE SCHEMA statement: %s", sql)
	}

	return &UseSchemaStmt{
		Name: normalizeIdentifier(matches[1]),
	}, nil
}

// ShowSchemasStmt represents a parsed SHOW SCHEMAS statement.
type ShowSchemasStmt struct {
	Database string // empty if not specified
}

// ShowTablesStmt represents a parsed SHOW TABLES statement.
type ShowTablesStmt struct {
	Database string // empty if not specified
	Schema   string // empty if not specified
}

// DescribeTableStmt represents a parsed DESCRIBE TABLE statement.
type DescribeTableStmt struct {
	Database string // empty if not specified
	Schema   string // empty if not specified
	Table    string
}

// Regex patterns for SHOW/DESCRIBE parsing.
var (
	showSchemasRe   = regexp.MustCompile(`(?i)^\s*SHOW\s+SCHEMAS(?:\s+IN\s+(?:DATABASE\s+)?("?[A-Za-z_][A-Za-z0-9_]*"?))?\s*;?\s*$`)
	showTablesRe    = regexp.MustCompile(`(?i)^\s*SHOW\s+TABLES(?:\s+IN\s+(?:("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?))?\s*;?\s*$`)
	describeTableRe = regexp.MustCompile(`(?i)^\s*(?:DESCRIBE|DESC)\s+TABLE\s+(?:("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?(?:("?[A-Za-z_][A-Za-z0-9_]*"?)\.)?("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$`)
)

// ParseShowSchemas parses a SHOW SCHEMAS statement.
// Supports: SHOW SCHEMAS, SHOW SCHEMAS IN DATABASE name, SHOW SCHEMAS IN name
func ParseShowSchemas(sql string) (*ShowSchemasStmt, error) {
	matches := showSchemasRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid SHOW SCHEMAS statement: %s", sql)
	}

	stmt := &ShowSchemasStmt{}
	if matches[1] != "" {
		stmt.Database = normalizeIdentifier(matches[1])
	}

	return stmt, nil
}

// ParseShowTables parses a SHOW TABLES statement.
// Supports: SHOW TABLES, SHOW TABLES IN schema, SHOW TABLES IN db.schema
func ParseShowTables(sql string) (*ShowTablesStmt, error) {
	matches := showTablesRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid SHOW TABLES statement: %s", sql)
	}

	stmt := &ShowTablesStmt{}
	if matches[1] != "" {
		stmt.Database = normalizeIdentifier(matches[1])
	}
	if matches[2] != "" {
		stmt.Schema = normalizeIdentifier(matches[2])
	}

	return stmt, nil
}

// ParseDescribeTable parses a DESCRIBE TABLE or DESC TABLE statement.
// Supports: DESCRIBE TABLE table, DESCRIBE TABLE schema.table, DESCRIBE TABLE db.schema.table
func ParseDescribeTable(sql string) (*DescribeTableStmt, error) {
	matches := describeTableRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid DESCRIBE TABLE statement: %s", sql)
	}

	stmt := &DescribeTableStmt{}

	// Three capture groups: optional prefix1, optional prefix2, required table
	// With 3 parts (db.schema.table): matches[1]=db, matches[2]=schema, matches[3]=table
	// With 2 parts (schema.table): matches[1]=schema, matches[2]="", matches[3]=table
	// With 1 part (table): matches[1]="", matches[2]="", matches[3]=table
	if matches[1] != "" && matches[2] != "" {
		// Three-part: db.schema.table
		stmt.Database = normalizeIdentifier(matches[1])
		stmt.Schema = normalizeIdentifier(matches[2])
	} else if matches[1] != "" {
		// Two-part: schema.table
		stmt.Schema = normalizeIdentifier(matches[1])
	}
	stmt.Table = normalizeIdentifier(matches[3])

	return stmt, nil
}

// CreateStreamStmt represents a parsed CREATE STREAM statement.
type CreateStreamStmt struct {
	Schema          string // schema of the stream (empty if not specified)
	Name            string
	SourceSchema    string // schema of source table (empty if not specified)
	SourceTable     string
	AppendOnly      bool
	ShowInitialRows bool
	IfNotExists     bool
}

// DropStreamStmt represents a parsed DROP STREAM statement.
type DropStreamStmt struct {
	Schema   string // empty if not specified
	Name     string
	IfExists bool
}

// ParseCreateStream parses a CREATE STREAM statement.
// Supports: CREATE STREAM [IF NOT EXISTS] [schema.]name ON TABLE [schema.]table [APPEND_ONLY = TRUE/FALSE] [SHOW_INITIAL_ROWS = TRUE/FALSE]
func ParseCreateStream(sql string) (*CreateStreamStmt, error) {
	matches := createStreamRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE STREAM statement: %s", sql)
	}

	stmt := &CreateStreamStmt{
		Name:        normalizeIdentifier(matches[4]),
		SourceTable: normalizeIdentifier(matches[8]),
		IfNotExists: matches[1] != "",
	}

	// Stream schema (matches[3])
	if matches[3] != "" {
		stmt.Schema = normalizeIdentifier(matches[3])
	}

	// Source table schema — could be in matches[6] (two-part) or matches[7] (three-part prefix)
	if matches[7] != "" {
		stmt.SourceSchema = normalizeIdentifier(matches[7])
	} else if matches[6] != "" {
		stmt.SourceSchema = normalizeIdentifier(matches[6])
	}

	// Parse trailing options (matches[9])
	options := strings.ToUpper(matches[9])
	if strings.Contains(options, "APPEND_ONLY") {
		stmt.AppendOnly = strings.Contains(options, "APPEND_ONLY = TRUE") || strings.Contains(options, "APPEND_ONLY=TRUE")
	}
	if strings.Contains(options, "SHOW_INITIAL_ROWS") {
		stmt.ShowInitialRows = strings.Contains(options, "SHOW_INITIAL_ROWS = TRUE") || strings.Contains(options, "SHOW_INITIAL_ROWS=TRUE")
	}

	return stmt, nil
}

// ParseDropStream parses a DROP STREAM statement.
// Supports: DROP STREAM [IF EXISTS] [schema.]name
func ParseDropStream(sql string) (*DropStreamStmt, error) {
	matches := dropStreamRe.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("invalid DROP STREAM statement: %s", sql)
	}

	stmt := &DropStreamStmt{
		Name:     normalizeIdentifier(matches[4]),
		IfExists: matches[1] != "",
	}

	if matches[3] != "" {
		stmt.Schema = normalizeIdentifier(matches[3])
	}

	return stmt, nil
}
