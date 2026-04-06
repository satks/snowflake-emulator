// Package query provides SQL query execution against DuckDB with Snowflake SQL translation.
package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/nnnkkk7/snowflake-emulator/pkg/connection"
	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// StreamProcessor handles Snowflake stream (CDC) operations.
// Streams are emulated using shadow changelog tables that track DML operations.
type StreamProcessor struct {
	mgr  *connection.Manager
	repo *metadata.Repository
}

// NewStreamProcessor creates a new stream processor.
func NewStreamProcessor(mgr *connection.Manager, repo *metadata.Repository) *StreamProcessor {
	return &StreamProcessor{
		mgr:  mgr,
		repo: repo,
	}
}

// ExecuteCreateStream creates a stream and its changelog table.
func (sp *StreamProcessor) ExecuteCreateStream(ctx context.Context, stmt *CreateStreamStmt, schemaID string) (*ExecResult, error) {
	// Resolve source table
	sourceTable, err := sp.repo.GetTableByName(ctx, schemaID, stmt.SourceTable)
	if err != nil {
		// Try resolving with source schema if specified
		if stmt.SourceSchema != "" {
			db, schema, resolveErr := sp.resolveSchema(ctx, "", stmt.SourceSchema)
			if resolveErr != nil {
				return nil, fmt.Errorf("source table %s not found: %w", stmt.SourceTable, err)
			}
			sourceTable, err = sp.repo.GetTableByName(ctx, schema.ID, stmt.SourceTable)
			if err != nil {
				return nil, fmt.Errorf("source table %s.%s not found: %w", stmt.SourceSchema, stmt.SourceTable, err)
			}
			_ = db
		} else {
			return nil, fmt.Errorf("source table %s not found: %w", stmt.SourceTable, err)
		}
	}

	// Build changelog table name
	changelogName := fmt.Sprintf("_stream_%s_changelog", strings.ToUpper(stmt.Name))

	// Get source table's schema and database for building the changelog table in DuckDB
	schema, err := sp.repo.GetSchema(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	db, err := sp.repo.GetDatabase(ctx, schema.DatabaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	// Parse source table column definitions to build changelog DDL
	var sourceColDefs []string
	if sourceTable.ColumnDefinitions != "" {
		for _, part := range strings.Split(sourceTable.ColumnDefinitions, ";") {
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 5)
			if len(fields) < 2 {
				continue
			}
			colDef := fmt.Sprintf(`"%s" %s`, fields[0], fields[1])
			sourceColDefs = append(sourceColDefs, colDef)
		}
	}

	// Add METADATA$ columns + event tracking
	allColDefs := append(sourceColDefs,
		`"METADATA$ACTION" VARCHAR`,
		`"METADATA$ISUPDATE" BOOLEAN`,
		`"METADATA$ROW_ID" VARCHAR`,
		`"_event_id" BIGINT DEFAULT 0`,
	)

	// Build fully qualified changelog table name
	fqChangelogName := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, changelogName)

	// Create the changelog table in DuckDB
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqChangelogName, strings.Join(allColDefs, ", "))
	if _, err := sp.mgr.Exec(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("failed to create changelog table: %w", err)
	}

	// If SHOW_INITIAL_ROWS, snapshot existing data into changelog
	if stmt.ShowInitialRows {
		// Get source table's fully qualified name
		fqSourceName := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, sourceTable.Name)

		// Build column name list (without METADATA$ columns)
		var colNames []string
		for _, part := range strings.Split(sourceTable.ColumnDefinitions, ";") {
			if part == "" {
				continue
			}
			fields := strings.SplitN(part, ":", 5)
			if len(fields) >= 1 {
				colNames = append(colNames, fmt.Sprintf(`"%s"`, fields[0]))
			}
		}

		if len(colNames) > 0 {
			snapshotSQL := fmt.Sprintf(
				`INSERT INTO %s (%s, "METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID", "_event_id") SELECT %s, 'INSERT', FALSE, uuid(), row_number() OVER () FROM %s`,
				fqChangelogName,
				strings.Join(colNames, ", "),
				strings.Join(colNames, ", "),
				fqSourceName,
			)
			// Best-effort: ignore errors if source table is empty or has schema mismatch
			_, _ = sp.mgr.Exec(ctx, snapshotSQL)
		}
	}

	// Create stream metadata
	_, err = sp.repo.CreateStream(ctx, schemaID, stmt.Name, sourceTable.ID, stmt.AppendOnly, stmt.ShowInitialRows, fqChangelogName, "")
	if err != nil {
		if stmt.IfNotExists && strings.Contains(err.Error(), "already exists") {
			return &ExecResult{RowsAffected: 0}, nil
		}
		// Cleanup: drop changelog table
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fqChangelogName)
		_, _ = sp.mgr.Exec(ctx, dropSQL)
		return nil, fmt.Errorf("failed to create stream metadata: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// ExecuteDropStream drops a stream and its changelog table.
func (sp *StreamProcessor) ExecuteDropStream(ctx context.Context, stmt *DropStreamStmt, schemaID string) (*ExecResult, error) {
	stream, err := sp.repo.GetStreamByName(ctx, schemaID, stmt.Name)
	if err != nil {
		if stmt.IfExists {
			return &ExecResult{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("stream %s not found: %w", stmt.Name, err)
	}

	// Drop the changelog table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", stream.ChangelogTable)
	if _, err := sp.mgr.Exec(ctx, dropSQL); err != nil {
		return nil, fmt.Errorf("failed to drop changelog table: %w", err)
	}

	// Delete stream metadata
	if err := sp.repo.DropStream(ctx, stream.ID); err != nil {
		return nil, fmt.Errorf("failed to drop stream metadata: %w", err)
	}

	return &ExecResult{RowsAffected: 0}, nil
}

// QueryStreamHasData checks if a stream has unconsumed changes (event_id > current_offset).
func (sp *StreamProcessor) QueryStreamHasData(ctx context.Context, streamName, schemaID string) (bool, error) {
	stream, err := sp.repo.GetStreamByName(ctx, schemaID, streamName)
	if err != nil {
		return false, fmt.Errorf("stream %s not found: %w", streamName, err)
	}

	// Check if there are events after the current offset
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "_event_id" > %d`, stream.ChangelogTable, stream.CurrentOffset)
	rows, err := sp.mgr.Query(ctx, countSQL)
	if err != nil {
		return false, fmt.Errorf("failed to query changelog: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, err
		}
	}

	return count > 0, nil
}

// resolveSchema resolves a database name and schema name to metadata objects.
func (sp *StreamProcessor) resolveSchema(ctx context.Context, dbName, schemaName string) (*metadata.Database, *metadata.Schema, error) {
	if dbName != "" {
		db, err := sp.repo.GetDatabaseByName(ctx, dbName)
		if err != nil {
			return nil, nil, err
		}
		schema, err := sp.repo.GetSchemaByName(ctx, db.ID, schemaName)
		if err != nil {
			return nil, nil, err
		}
		return db, schema, nil
	}

	// No database specified — search all databases
	databases, err := sp.repo.ListDatabases(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, db := range databases {
		schema, err := sp.repo.GetSchemaByName(ctx, db.ID, schemaName)
		if err == nil {
			return db, schema, nil
		}
	}
	return nil, nil, fmt.Errorf("schema %s not found", schemaName)
}

// RecordDMLChange records a DML change to all streams tracking the given table.
// For a test emulator, we use a simplified approach: after DML, capture the current
// state of affected rows. For INSERT, the new rows are captured. For DELETE/UPDATE,
// we record the action type. The changelog uses event_id for offset tracking.
func (sp *StreamProcessor) RecordDMLChange(ctx context.Context, tableName, action string, isUpdate bool) {
	// Find all tables matching the name across all schemas
	databases, err := sp.repo.ListDatabases(ctx)
	if err != nil {
		return
	}

	for _, db := range databases {
		schemas, err := sp.repo.ListSchemas(ctx, db.ID)
		if err != nil {
			continue
		}
		for _, schema := range schemas {
			table, err := sp.repo.GetTableByName(ctx, schema.ID, tableName)
			if err != nil {
				continue
			}

			streams, err := sp.repo.GetStreamsBySourceTable(ctx, table.ID)
			if err != nil || len(streams) == 0 {
				continue
			}

			// For APPEND_ONLY streams, skip DELETE/UPDATE
			for _, stream := range streams {
				if stream.AppendOnly && action != "INSERT" {
					continue
				}

				// Get max event_id for this changelog
				maxEventSQL := fmt.Sprintf(`SELECT COALESCE(MAX("_event_id"), 0) FROM %s`, stream.ChangelogTable)
				rows, err := sp.mgr.Query(ctx, maxEventSQL)
				if err != nil {
					continue
				}
				var maxEvent int64
				if rows.Next() {
					_ = rows.Scan(&maxEvent)
				}
				_ = rows.Close()
				nextEvent := maxEvent + 1

				// Build source column list
				var colNames []string
				if table.ColumnDefinitions != "" {
					for _, part := range strings.Split(table.ColumnDefinitions, ";") {
						if part == "" {
							continue
						}
						fields := strings.SplitN(part, ":", 5)
						if len(fields) >= 1 {
							colNames = append(colNames, fmt.Sprintf(`"%s"`, fields[0]))
						}
					}
				}

				if len(colNames) == 0 {
					continue
				}

				// Source table FQ name
				fqSourceName := fmt.Sprintf("%s.%s_%s", db.Name, schema.Name, table.Name)

				// For INSERT: snapshot current table state into changelog
				// For DELETE/UPDATE: also snapshot (simplified for test emulator)
				insertSQL := fmt.Sprintf(
					`INSERT INTO %s (%s, "METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID", "_event_id") SELECT %s, '%s', %t, uuid(), %d FROM %s`,
					stream.ChangelogTable,
					strings.Join(colNames, ", "),
					strings.Join(colNames, ", "),
					action,
					isUpdate,
					nextEvent,
					fqSourceName,
				)
				// Best-effort: don't fail the original DML if changelog write fails
				_, _ = sp.mgr.Exec(ctx, insertSQL)
			}
		}
	}
}

// streamHasDataRe matches SYSTEM$STREAM_HAS_DATA('stream_name') or SYSTEM$STREAM_HAS_DATA('"schema"."stream"')
var streamHasDataRe = regexp.MustCompile(`(?i)SYSTEM\$STREAM_HAS_DATA\s*\(\s*['"](.+?)['"]\s*\)`)

// ParseStreamHasData extracts the stream name from SYSTEM$STREAM_HAS_DATA() call.
func ParseStreamHasData(sql string) (string, error) {
	matches := streamHasDataRe.FindStringSubmatch(sql)
	if matches == nil {
		return "", fmt.Errorf("no SYSTEM$STREAM_HAS_DATA found in: %s", sql)
	}
	// Strip any remaining quotes
	name := strings.Trim(matches[1], `"'`)
	return name, nil
}
