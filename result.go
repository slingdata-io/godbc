package godbc

import (
	"database/sql/driver"
)

// Result implements driver.Result for INSERT, UPDATE, DELETE operations
type Result struct {
	lastInsertId int64
	rowsAffected int64
	outputParams []interface{}
}

// LastInsertId returns the ID of the last inserted row.
// When LastInsertIdAuto behavior is configured (default), this automatically
// executes the appropriate identity query for the connected database type.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

// RowsAffected returns the number of rows affected by the query
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// OutputParams returns the values of output parameters after executing a stored procedure.
// The values are returned in the same order as the parameters were bound.
// Only parameters marked as ParamOutput or ParamInputOutput will have values.
// Input-only parameters will have nil values in the corresponding positions.
func (r *Result) OutputParams() []interface{} {
	return r.outputParams
}

// OutputParam returns a single output parameter value by index (0-based).
// Returns nil if the index is out of range or if the parameter was input-only.
func (r *Result) OutputParam(index int) interface{} {
	if index < 0 || index >= len(r.outputParams) {
		return nil
	}
	return r.outputParams[index]
}

// Ensure Result implements driver.Result
var _ driver.Result = (*Result)(nil)
