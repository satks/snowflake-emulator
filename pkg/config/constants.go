// Package config provides configuration constants for the Snowflake emulator.
package config

import "os"

// Default database and schema settings.
const (
	DefaultDatabase = "TEST_DB"
	DefaultSchema   = "PUBLIC"
)

// StatementTypeID represents Snowflake statement type identifiers.
type StatementTypeID int64

// Statement type IDs for gosnowflake protocol.
const (
	StatementTypeSelect StatementTypeID = 1
	StatementTypeInsert StatementTypeID = 2
	StatementTypeDML    StatementTypeID = 3
	StatementTypeDDL    StatementTypeID = 4
	StatementTypeDrop   StatementTypeID = 5
)

// QueryResultFormat defines the format of query results.
const (
	QueryResultFormatJSON = "json"
)

// Session parameter defaults.
const (
	DefaultTimezone               = "UTC"
	DefaultTimestampOutputFormat  = "YYYY-MM-DD HH24:MI:SS"
	DefaultClientSessionKeepAlive = "false"
	DefaultQueryTag               = ""
)

// SessionParameter represents a session parameter name.
type SessionParameter string

// Session parameter names.
const (
	ParamTimezone               SessionParameter = "TIMEZONE"
	ParamTimestampOutputFormat  SessionParameter = "TIMESTAMP_OUTPUT_FORMAT"
	ParamClientSessionKeepAlive SessionParameter = "CLIENT_SESSION_KEEP_ALIVE"
	ParamQueryTag               SessionParameter = "QUERY_TAG"
	ParamGoQueryResultFormat    SessionParameter = "GO_QUERY_RESULT_FORMAT"
)

// IsCatalogMode returns true if catalog mode is enabled via ENABLE_CATALOG_MODE env var.
// When enabled, databases are created as DuckDB catalogs (via ATTACH) enabling
// three-part name resolution (database.schema.table).
func IsCatalogMode() bool {
	return os.Getenv("ENABLE_CATALOG_MODE") == "true"
}

// DefaultSessionParameters returns the default session parameters.
func DefaultSessionParameters() map[SessionParameter]string {
	return map[SessionParameter]string{
		ParamTimezone:               DefaultTimezone,
		ParamTimestampOutputFormat:  DefaultTimestampOutputFormat,
		ParamClientSessionKeepAlive: DefaultClientSessionKeepAlive,
		ParamQueryTag:               DefaultQueryTag,
		ParamGoQueryResultFormat:    QueryResultFormatJSON,
	}
}
