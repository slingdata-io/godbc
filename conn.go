package odbc

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

// Conn implements driver.Conn and represents a connection to a database
type Conn struct {
	env    SQLHENV
	dbc    SQLHDBC
	inTx   bool
	mu     sync.Mutex
	closed bool
}

// Prepare prepares a statement for execution
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext prepares a statement with context support
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Allocate statement handle
	var stmtHandle SQLHSTMT
	ret := AllocHandle(SQL_HANDLE_STMT, SQLHANDLE(c.dbc), (*SQLHANDLE)(&stmtHandle))
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
	}

	// Prepare the statement
	ret = Prepare(stmtHandle, query)
	if !IsSuccess(ret) {
		err := NewError(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))
		FreeHandle(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))
		return nil, err
	}

	// Get number of parameters
	var numParams SQLSMALLINT
	ret = NumParams(stmtHandle, &numParams)
	if !IsSuccess(ret) {
		// Non-fatal: some drivers don't support NumParams, default to -1 (unknown)
		numParams = -1
	}

	stmt := &Stmt{
		conn:     c,
		stmt:     stmtHandle,
		query:    query,
		numInput: int(numParams),
	}

	return stmt, nil
}

// Close closes the connection
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Disconnect and free handles
	if c.dbc != 0 {
		Disconnect(c.dbc)
		FreeHandle(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
		c.dbc = 0
	}
	if c.env != 0 {
		FreeHandle(SQL_HANDLE_ENV, SQLHANDLE(c.env))
		c.env = 0
	}

	return nil
}

// Begin starts a new transaction (deprecated, use BeginTx)
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a new transaction with context and options
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	if c.inTx {
		return nil, errors.New("already in a transaction")
	}

	// Set transaction isolation level if specified
	if opts.Isolation != 0 {
		var isoLevel uintptr
		switch driver.IsolationLevel(opts.Isolation) {
		case driver.IsolationLevel(1): // LevelReadUncommitted
			isoLevel = SQL_TXN_READ_UNCOMMITTED
		case driver.IsolationLevel(2): // LevelReadCommitted
			isoLevel = SQL_TXN_READ_COMMITTED
		case driver.IsolationLevel(3): // LevelWriteCommitted (not standard, use read committed)
			isoLevel = SQL_TXN_READ_COMMITTED
		case driver.IsolationLevel(4): // LevelRepeatableRead
			isoLevel = SQL_TXN_REPEATABLE_READ
		case driver.IsolationLevel(5): // LevelSnapshot (use serializable as fallback)
			isoLevel = SQL_TXN_SERIALIZABLE
		case driver.IsolationLevel(6): // LevelSerializable
			isoLevel = SQL_TXN_SERIALIZABLE
		case driver.IsolationLevel(7): // LevelLinearizable (use serializable)
			isoLevel = SQL_TXN_SERIALIZABLE
		default:
			isoLevel = SQL_TXN_READ_COMMITTED
		}
		ret := SetConnectAttr(c.dbc, SQL_ATTR_TXN_ISOLATION, isoLevel, 0)
		if !IsSuccess(ret) {
			return nil, NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
		}
	}

	// Set read-only mode if requested
	if opts.ReadOnly {
		ret := SetConnectAttr(c.dbc, SQL_ATTR_ACCESS_MODE, SQL_MODE_READ_ONLY, 0)
		if !IsSuccess(ret) {
			return nil, NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
		}
	}

	// Disable autocommit to start transaction
	ret := SetConnectAttr(c.dbc, SQL_ATTR_AUTOCOMMIT, uintptr(SQL_AUTOCOMMIT_OFF), 0)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
	}

	c.inTx = true
	return &Tx{conn: c}, nil
}

// Ping verifies the connection is still alive
func (c *Conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// Allocate a temporary statement handle
	var stmtHandle SQLHSTMT
	ret := AllocHandle(SQL_HANDLE_STMT, SQLHANDLE(c.dbc), (*SQLHANDLE)(&stmtHandle))
	if !IsSuccess(ret) {
		return driver.ErrBadConn
	}
	defer FreeHandle(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))

	// Execute a simple query to verify connection
	ret = ExecDirect(stmtHandle, "SELECT 1")
	if !IsSuccess(ret) {
		// Check if it's a connection error
		if err := NewError(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle)); IsConnectionError(err) {
			return driver.ErrBadConn
		}
		// Some databases don't support "SELECT 1", try just allocating a handle
		// If the handle allocation succeeded, the connection is likely fine
		return nil
	}

	return nil
}

// ExecContext executes a query without returning rows
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// If no args, use direct execution
	if len(args) == 0 {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, driver.ErrBadConn
		}

		var stmtHandle SQLHSTMT
		ret := AllocHandle(SQL_HANDLE_STMT, SQLHANDLE(c.dbc), (*SQLHANDLE)(&stmtHandle))
		if !IsSuccess(ret) {
			err := NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
			c.mu.Unlock()
			return nil, err
		}
		c.mu.Unlock()
		defer FreeHandle(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))

		ret = ExecDirect(stmtHandle, query)
		if !IsSuccess(ret) && ret != SQL_NO_DATA {
			return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))
		}

		var rowCount SQLLEN
		RowCount(stmtHandle, &rowCount)

		return &Result{rowsAffected: int64(rowCount)}, nil
	}

	// Use prepared statement for parameterized queries
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.(*Stmt).ExecContext(ctx, args)
}

// QueryContext executes a query that returns rows
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// If no args, use direct execution
	if len(args) == 0 {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, driver.ErrBadConn
		}

		var stmtHandle SQLHSTMT
		ret := AllocHandle(SQL_HANDLE_STMT, SQLHANDLE(c.dbc), (*SQLHANDLE)(&stmtHandle))
		if !IsSuccess(ret) {
			err := NewError(SQL_HANDLE_DBC, SQLHANDLE(c.dbc))
			c.mu.Unlock()
			return nil, err
		}
		c.mu.Unlock()

		ret = ExecDirect(stmtHandle, query)
		if !IsSuccess(ret) {
			err := NewError(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))
			FreeHandle(SQL_HANDLE_STMT, SQLHANDLE(stmtHandle))
			return nil, err
		}

		// Create a temporary stmt wrapper for rows
		stmt := &Stmt{
			conn:  c,
			stmt:  stmtHandle,
			query: query,
		}
		return newRows(stmt, true) // closeStmt=true since we own the handle
	}

	// Use prepared statement for parameterized queries
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.(*Stmt).QueryContext(ctx, args)
	if err != nil {
		stmt.Close()
		return nil, err
	}
	// Set closeStmt on rows so statement is closed when rows are closed
	rows.(*Rows).closeStmt = true
	return rows, nil
}

// ResetSession is called before a connection is reused
func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// If still in a transaction, the connection is in a bad state
	if c.inTx {
		return driver.ErrBadConn
	}

	return nil
}

// IsValid returns true if the connection is valid
func (c *Conn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.dbc != 0
}

// CheckNamedValue validates and converts named values
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Use the default converter for now
	return nil
}

// Ensure Conn implements the required interfaces
var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.Validator          = (*Conn)(nil)
)
