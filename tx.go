package odbc

import (
	"database/sql/driver"
)

// Tx implements driver.Tx for transaction support
type Tx struct {
	conn *Conn
}

// Commit commits the transaction.
// If the commit succeeds, autocommit is re-enabled for subsequent operations.
func (t *Tx) Commit() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()

	if !t.conn.inTx {
		return nil // Already committed or rolled back
	}

	ret := EndTran(SQL_HANDLE_DBC, SQLHANDLE(t.conn.dbc), SQL_COMMIT)
	t.conn.inTx = false

	// Check commit result first
	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_DBC, SQLHANDLE(t.conn.dbc))
	}

	// Re-enable autocommit (commit succeeded, so this is best-effort)
	SetConnectAttr(t.conn.dbc, SQL_ATTR_AUTOCOMMIT, uintptr(SQL_AUTOCOMMIT_ON), 0)

	// Reset access mode to read-write (best-effort)
	SetConnectAttr(t.conn.dbc, SQL_ATTR_ACCESS_MODE, SQL_MODE_READ_WRITE, 0)

	return nil
}

// Rollback rolls back the transaction.
// If the rollback succeeds, autocommit is re-enabled for subsequent operations.
func (t *Tx) Rollback() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()

	if !t.conn.inTx {
		return nil // Already committed or rolled back
	}

	ret := EndTran(SQL_HANDLE_DBC, SQLHANDLE(t.conn.dbc), SQL_ROLLBACK)
	t.conn.inTx = false

	// Check rollback result first
	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_DBC, SQLHANDLE(t.conn.dbc))
	}

	// Re-enable autocommit (rollback succeeded, so this is best-effort)
	SetConnectAttr(t.conn.dbc, SQL_ATTR_AUTOCOMMIT, uintptr(SQL_AUTOCOMMIT_ON), 0)

	// Reset access mode to read-write (best-effort)
	SetConnectAttr(t.conn.dbc, SQL_ATTR_ACCESS_MODE, SQL_MODE_READ_WRITE, 0)

	return nil
}

// Ensure Tx implements driver.Tx
var _ driver.Tx = (*Tx)(nil)
