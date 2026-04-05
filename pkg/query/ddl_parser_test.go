package query

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseCreateDatabase(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *CreateDatabaseStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "CREATE DATABASE test",
			want: &CreateDatabaseStmt{Name: "TEST", IfNotExists: false},
		},
		{
			name: "if_not_exists",
			sql:  "CREATE DATABASE IF NOT EXISTS test",
			want: &CreateDatabaseStmt{Name: "TEST", IfNotExists: true},
		},
		{
			name: "quoted",
			sql:  `CREATE DATABASE "MyDB"`,
			want: &CreateDatabaseStmt{Name: "MyDB", IfNotExists: false},
		},
		{
			name: "quoted_if_not_exists",
			sql:  `CREATE DATABASE IF NOT EXISTS "MyDB"`,
			want: &CreateDatabaseStmt{Name: "MyDB", IfNotExists: true},
		},
		{
			name: "with_semicolon",
			sql:  "CREATE DATABASE test;",
			want: &CreateDatabaseStmt{Name: "TEST", IfNotExists: false},
		},
		{
			name: "uppercase",
			sql:  "CREATE DATABASE TEST_DB",
			want: &CreateDatabaseStmt{Name: "TEST_DB", IfNotExists: false},
		},
		{
			name:    "invalid",
			sql:     "CREATE TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCreateDatabase(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCreateDatabase() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseCreateDatabase() mismatch:\n%s", diff)
				}
			}
		})
	}
}

func TestParseDropDatabase(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *DropDatabaseStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "DROP DATABASE test",
			want: &DropDatabaseStmt{Name: "TEST", IfExists: false},
		},
		{
			name: "if_exists",
			sql:  "DROP DATABASE IF EXISTS test",
			want: &DropDatabaseStmt{Name: "TEST", IfExists: true},
		},
		{
			name: "quoted",
			sql:  `DROP DATABASE "MyDB"`,
			want: &DropDatabaseStmt{Name: "MyDB", IfExists: false},
		},
		{
			name:    "invalid",
			sql:     "DROP TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDropDatabase(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDropDatabase() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseDropDatabase() mismatch:\n%s", diff)
				}
			}
		})
	}
}

func TestParseCreateSchema(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *CreateSchemaStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "CREATE SCHEMA myschema",
			want: &CreateSchemaStmt{Schema: "MYSCHEMA", IfNotExists: false},
		},
		{
			name: "with_database",
			sql:  "CREATE SCHEMA test.myschema",
			want: &CreateSchemaStmt{Database: "TEST", Schema: "MYSCHEMA", IfNotExists: false},
		},
		{
			name: "if_not_exists_with_database",
			sql:  "CREATE SCHEMA IF NOT EXISTS test.e2e_test_001",
			want: &CreateSchemaStmt{Database: "TEST", Schema: "E2E_TEST_001", IfNotExists: true},
		},
		{
			name: "quoted",
			sql:  `CREATE SCHEMA "TEST"."MySchema"`,
			want: &CreateSchemaStmt{Database: "TEST", Schema: "MySchema", IfNotExists: false},
		},
		{
			name: "if_not_exists_no_database",
			sql:  "CREATE SCHEMA IF NOT EXISTS myschema",
			want: &CreateSchemaStmt{Schema: "MYSCHEMA", IfNotExists: true},
		},
		{
			name:    "invalid",
			sql:     "CREATE TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCreateSchema(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCreateSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseCreateSchema() mismatch:\n%s", diff)
				}
			}
		})
	}
}

func TestParseDropSchema(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *DropSchemaStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "DROP SCHEMA myschema",
			want: &DropSchemaStmt{Schema: "MYSCHEMA", IfExists: false},
		},
		{
			name: "if_exists_with_database",
			sql:  "DROP SCHEMA IF EXISTS test.myschema",
			want: &DropSchemaStmt{Database: "TEST", Schema: "MYSCHEMA", IfExists: true},
		},
		{
			name: "quoted",
			sql:  `DROP SCHEMA "TEST"."MySchema"`,
			want: &DropSchemaStmt{Database: "TEST", Schema: "MySchema", IfExists: false},
		},
		{
			name:    "invalid",
			sql:     "DROP TABLE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDropSchema(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDropSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseDropSchema() mismatch:\n%s", diff)
				}
			}
		})
	}
}

func TestParseUseDatabase(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *UseDatabaseStmt
		wantErr bool
	}{
		{
			name: "simple",
			sql:  "USE DATABASE test",
			want: &UseDatabaseStmt{Name: "TEST"},
		},
		{
			name: "quoted",
			sql:  `USE DATABASE "MyDB"`,
			want: &UseDatabaseStmt{Name: "MyDB"},
		},
		{
			name: "with_semicolon",
			sql:  "USE DATABASE test;",
			want: &UseDatabaseStmt{Name: "TEST"},
		},
		{
			name:    "invalid",
			sql:     "USE SCHEMA test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUseDatabase(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUseDatabase() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseUseDatabase() mismatch:\n%s", diff)
				}
			}
		})
	}
}

func TestParseUseSchema(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    *UseSchemaStmt
		wantErr bool
	}{
		{
			name: "use_schema",
			sql:  "USE SCHEMA myschema",
			want: &UseSchemaStmt{Name: "MYSCHEMA"},
		},
		{
			name: "bare_use",
			sql:  "USE myschema",
			want: &UseSchemaStmt{Name: "MYSCHEMA"},
		},
		{
			name: "quoted",
			sql:  `USE SCHEMA "MySchema"`,
			want: &UseSchemaStmt{Name: "MySchema"},
		},
		{
			name:    "use_database_not_matched",
			sql:     "USE DATABASE test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUseSchema(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUseSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ParseUseSchema() mismatch:\n%s", diff)
				}
			}
		})
	}
}
