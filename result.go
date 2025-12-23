package odbc

import (
	"database/sql/driver"
)

// Result implements driver.Result for INSERT, UPDATE, DELETE operations
type Result struct {
	lastInsertId int64
	rowsAffected int64
}

// LastInsertId returns the ID of the last inserted row
// Note: ODBC doesn't have a standard way to get last insert ID.
// This varies by database. Some databases support SCOPE_IDENTITY(),
// @@IDENTITY, or RETURNING clauses.
func (r *Result) LastInsertId() (int64, error) {
	// Return the stored value if it was set
	if r.lastInsertId != 0 {
		return r.lastInsertId, nil
	}
	// ODBC doesn't provide a standard way to get last insert ID
	// Return 0 - callers should use database-specific methods like:
	// - SQL Server: SELECT SCOPE_IDENTITY() or OUTPUT clause
	// - MySQL: SELECT LAST_INSERT_ID()
	// - PostgreSQL: RETURNING clause
	// - SQLite: SELECT last_insert_rowid()
	return 0, nil
}

// RowsAffected returns the number of rows affected by the query
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Ensure Result implements driver.Result
var _ driver.Result = (*Result)(nil)
