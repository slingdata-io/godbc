package odbc

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
)

// maxParameters limits the number of parameters to prevent unbounded memory allocation.
const maxParameters = 10000

// Stmt implements driver.Stmt for prepared statements
type Stmt struct {
	conn     *Conn
	stmt     SQLHSTMT
	query    string
	numInput int
	mu       sync.Mutex
	closed   bool

	// Parameter buffers - kept alive during execution
	paramBuffers []interface{}
	paramLengths []SQLLEN
}

// Close closes the statement
func (s *Stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.stmt != 0 {
		FreeHandle(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
		s.stmt = 0
	}

	// Clear parameter buffers
	s.paramBuffers = nil
	s.paramLengths = nil

	return nil
}

// NumInput returns the number of placeholder parameters
func (s *Stmt) NumInput() int {
	return s.numInput
}

// Exec executes a prepared statement (deprecated, use ExecContext)
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.ExecContext(context.Background(), namedArgs)
}

// ExecContext executes a prepared statement with context
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Bind parameters
	if err := s.bindParams(args); err != nil {
		return nil, err
	}

	// Execute the statement
	ret := Execute(s.stmt)
	if !IsSuccess(ret) && ret != SQL_NO_DATA {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
	}

	// Get rows affected
	var rowCount SQLLEN
	RowCount(s.stmt, &rowCount)

	// Reset parameters for next execution
	FreeStmt(s.stmt, SQL_RESET_PARAMS)

	return &Result{rowsAffected: int64(rowCount)}, nil
}

// Query executes a prepared query (deprecated, use QueryContext)
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.QueryContext(context.Background(), namedArgs)
}

// QueryContext executes a prepared query with context
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Bind parameters
	if err := s.bindParams(args); err != nil {
		return nil, err
	}

	// Execute the statement
	ret := Execute(s.stmt)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
	}

	// Create rows - don't close stmt when rows close (we own it)
	return newRows(s, false)
}

// bindParams binds parameters to the statement
func (s *Stmt) bindParams(args []driver.NamedValue) error {
	// Clear previous parameter buffers
	s.paramBuffers = make([]interface{}, len(args))
	s.paramLengths = make([]SQLLEN, len(args))

	for _, arg := range args {
		paramNum := SQLUSMALLINT(arg.Ordinal)
		if paramNum == 0 {
			continue
		}

		if err := s.bindParam(paramNum, arg.Value); err != nil {
			return err
		}
	}

	return nil
}

// bindParam binds a single parameter
func (s *Stmt) bindParam(paramNum SQLUSMALLINT, value interface{}) error {
	idx := int(paramNum) - 1
	if idx < 0 {
		return fmt.Errorf("invalid parameter number %d: must be positive", paramNum)
	}
	if idx >= maxParameters {
		return fmt.Errorf("parameter number %d exceeds maximum %d", paramNum, maxParameters)
	}
	if idx >= len(s.paramBuffers) {
		// Extend slices if needed
		for len(s.paramBuffers) <= idx {
			s.paramBuffers = append(s.paramBuffers, nil)
			s.paramLengths = append(s.paramLengths, 0)
		}
	}

	// Convert value and bind
	buf, cType, sqlType, colSize, decDigits, length, err := convertToODBC(value)
	if err != nil {
		return err
	}

	// Store buffer to keep it alive
	s.paramBuffers[idx] = buf
	s.paramLengths[idx] = length

	// Get pointer to data
	var dataPtr uintptr
	var bufferLen SQLLEN
	if buf != nil {
		dataPtr, bufferLen = getBufferPtr(buf)
	}

	ret := BindParameter(
		s.stmt,
		paramNum,
		SQL_PARAM_INPUT,
		cType,
		sqlType,
		colSize,
		decDigits,
		dataPtr,
		bufferLen,
		&s.paramLengths[idx],
	)

	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
	}

	return nil
}

// Ensure Stmt implements the required interfaces
var (
	_ driver.Stmt             = (*Stmt)(nil)
	_ driver.StmtExecContext  = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
)
