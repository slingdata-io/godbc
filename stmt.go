package odbc

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// maxParameters limits the number of parameters to prevent unbounded memory allocation.
const maxParameters = 10000

// Default buffer sizes for output parameters
const (
	defaultStringBufferSize = 4000
	defaultBinaryBufferSize = 8000
)

// outputParamInfo tracks information about output parameters for retrieval
type outputParamInfo struct {
	index     int           // Parameter index (0-based)
	direction ParamDirection
	buffer    interface{}   // Buffer holding the output value
	length    *SQLLEN       // Length/indicator pointer
	cType     SQLSMALLINT   // C type for retrieval
	goType    interface{}   // Original Go type hint for conversion
}

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

	// Output parameter tracking
	outputParams []outputParamInfo

	// Cursor configuration
	cursorType CursorType

	// Named parameter support
	namedParams *NamedParams
}

// Close releases all resources associated with the prepared statement.
// It is safe to call Close multiple times; subsequent calls are no-ops.
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
	s.outputParams = nil

	return nil
}

// NumInput returns the number of placeholder parameters in the prepared statement.
// Returns -1 if the driver cannot determine the count.
func (s *Stmt) NumInput() int {
	return s.numInput
}

// Exec executes a prepared statement without returning rows.
// Deprecated: Use ExecContext with context support instead.
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

// ExecContext executes a prepared statement that doesn't return rows.
// It supports context cancellation and named/positional parameters.
// Returns a Result with rows affected and output parameter values.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Set query timeout if configured
	if s.conn.queryTimeout > 0 {
		timeoutSecs := int(s.conn.queryTimeout.Seconds())
		if timeoutSecs < 1 {
			timeoutSecs = 1
		}
		SetStmtAttr(s.stmt, SQL_ATTR_QUERY_TIMEOUT, uintptr(timeoutSecs), 0)
	}

	// Start cancellation goroutine if context has deadline/cancel
	if ctx.Done() != nil {
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				Cancel(s.stmt)
			case <-done:
			}
		}()
	}

	// Bind parameters
	if err := s.bindParams(args); err != nil {
		return nil, err
	}

	// Check context before executing
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Execute the statement
	ret := Execute(s.stmt)
	if !IsSuccess(ret) && ret != SQL_NO_DATA {
		// Check if cancelled by context
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
	}

	// Get rows affected
	var rowCount SQLLEN
	RowCount(s.stmt, &rowCount)

	// Retrieve output parameter values
	outputValues := s.retrieveOutputParams()

	// Get last insert ID if this looks like an INSERT statement
	var lastInsertId int64
	if s.conn.lastInsertIdBehavior == LastInsertIdAuto && isInsertStatement(s.query) {
		lastInsertId = s.conn.getLastInsertId()
	}

	// Reset parameters for next execution
	FreeStmt(s.stmt, SQL_RESET_PARAMS)
	s.outputParams = nil

	return &Result{
		rowsAffected: int64(rowCount),
		lastInsertId: lastInsertId,
		outputParams: outputValues,
	}, nil
}

// Query executes a prepared statement that returns rows.
// Deprecated: Use QueryContext with context support instead.
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

// QueryContext executes a prepared statement that returns rows.
// It supports context cancellation and named/positional parameters.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Set query timeout if configured
	if s.conn.queryTimeout > 0 {
		timeoutSecs := int(s.conn.queryTimeout.Seconds())
		if timeoutSecs < 1 {
			timeoutSecs = 1
		}
		SetStmtAttr(s.stmt, SQL_ATTR_QUERY_TIMEOUT, uintptr(timeoutSecs), 0)
	}

	// Start cancellation goroutine if context has deadline/cancel
	if ctx.Done() != nil {
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				Cancel(s.stmt)
			case <-done:
			}
		}()
	}

	// Bind parameters
	if err := s.bindParams(args); err != nil {
		return nil, err
	}

	// Check context before executing
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Execute the statement
	ret := Execute(s.stmt)
	if !IsSuccess(ret) {
		// Check if cancelled by context
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
	}

	// Create rows - don't close stmt when rows close (we own it)
	return newRows(s, false)
}

// bindParams binds parameters to the statement
func (s *Stmt) bindParams(args []driver.NamedValue) error {
	// Handle named parameters
	if s.namedParams != nil {
		return s.bindNamedParams(args)
	}

	// Clear previous parameter buffers
	s.paramBuffers = make([]interface{}, len(args))
	s.paramLengths = make([]SQLLEN, len(args))
	s.outputParams = nil

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

// bindNamedParams handles binding for named parameters
func (s *Stmt) bindNamedParams(args []driver.NamedValue) error {
	// Calculate total number of parameter positions needed
	totalPositions := 0
	for _, positions := range s.namedParams.Positions {
		if len(positions) > totalPositions {
			totalPositions = positions[len(positions)-1]
		}
	}

	// Clear previous parameter buffers
	s.paramBuffers = make([]interface{}, totalPositions)
	s.paramLengths = make([]SQLLEN, totalPositions)
	s.outputParams = nil

	// Build a map from parameter name to value for quick lookup
	valueByName := make(map[string]interface{})
	valueByOrdinal := make(map[int]interface{})

	for _, arg := range args {
		if arg.Name != "" {
			valueByName[arg.Name] = arg.Value
		} else if arg.Ordinal > 0 {
			valueByOrdinal[arg.Ordinal] = arg.Value
		}
	}

	// Bind each named parameter to all its positions
	for name, positions := range s.namedParams.Positions {
		// Look up value by name first
		value, ok := valueByName[name]
		if !ok {
			// Try to find by ordinal based on order in Names slice
			for idx, n := range s.namedParams.Names {
				if n == name {
					if v, exists := valueByOrdinal[idx+1]; exists {
						value = v
						ok = true
					}
					break
				}
			}
		}

		if !ok {
			return &ParameterError{Name: name, Message: "missing value for named parameter"}
		}

		// Bind the value to each position where this parameter appears
		for _, pos := range positions {
			if err := s.bindParam(SQLUSMALLINT(pos), value); err != nil {
				return err
			}
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

	// Check if this is an output parameter
	var direction ParamDirection = ParamInput
	var actualValue interface{} = value
	var outputSize int

	if op, ok := value.(OutputParam); ok {
		direction = op.Direction
		actualValue = op.Value
		outputSize = op.Size
	}

	// Determine ODBC parameter direction
	var odbcDirection SQLSMALLINT
	switch direction {
	case ParamOutput:
		odbcDirection = SQL_PARAM_OUTPUT
	case ParamInputOutput:
		odbcDirection = SQL_PARAM_INPUT_OUTPUT
	default:
		odbcDirection = SQL_PARAM_INPUT
	}

	// For output parameters, we need to allocate appropriate buffers
	var buf interface{}
	var cType, sqlType SQLSMALLINT
	var colSize SQLULEN
	var decDigits SQLSMALLINT
	var length SQLLEN
	var err error

	if direction == ParamOutput || direction == ParamInputOutput {
		buf, cType, sqlType, colSize, decDigits, length, err = s.allocateOutputBuffer(actualValue, outputSize, direction)
	} else {
		buf, cType, sqlType, colSize, decDigits, length, err = convertToODBC(actualValue)
	}
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
		odbcDirection,
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

	// Track output parameters for later retrieval
	if direction == ParamOutput || direction == ParamInputOutput {
		s.outputParams = append(s.outputParams, outputParamInfo{
			index:     idx,
			direction: direction,
			buffer:    buf,
			length:    &s.paramLengths[idx],
			cType:     cType,
			goType:    actualValue,
		})
	}

	return nil
}

// allocateOutputBuffer creates a buffer suitable for output parameter binding
func (s *Stmt) allocateOutputBuffer(typeHint interface{}, size int, direction ParamDirection) (interface{}, SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLLEN, error) {
	// For input/output, we use the value both as type hint and initial value
	// For output-only, the value is just a type hint

	switch v := typeHint.(type) {
	case nil:
		// Default to string for nil type hint
		bufSize := size
		if bufSize == 0 {
			bufSize = defaultStringBufferSize
		}
		buf := make([]byte, bufSize+1) // +1 for null terminator
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(bufSize), 0, SQL_NULL_DATA, nil

	case bool:
		b := new(byte)
		if direction == ParamInputOutput && v {
			*b = 1
		}
		return b, SQL_C_BIT, SQL_BIT, 1, 0, 1, nil

	case int:
		val := new(int64)
		if direction == ParamInputOutput {
			*val = int64(v)
		}
		return val, SQL_C_SBIGINT, SQL_BIGINT, 20, 0, 8, nil

	case int8:
		val := new(int8)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_STINYINT, SQL_TINYINT, 4, 0, 1, nil

	case int16:
		val := new(int16)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_SSHORT, SQL_SMALLINT, 6, 0, 2, nil

	case int32:
		val := new(int32)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_SLONG, SQL_INTEGER, 11, 0, 4, nil

	case int64:
		val := new(int64)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_SBIGINT, SQL_BIGINT, 20, 0, 8, nil

	case float32:
		val := new(float32)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_FLOAT, SQL_REAL, 7, 0, 4, nil

	case float64:
		val := new(float64)
		if direction == ParamInputOutput {
			*val = v
		}
		return val, SQL_C_DOUBLE, SQL_DOUBLE, 15, 0, 8, nil

	case string:
		bufSize := size
		if bufSize == 0 {
			bufSize = defaultStringBufferSize
		}
		buf := make([]byte, bufSize+1) // +1 for null terminator
		if direction == ParamInputOutput && len(v) > 0 {
			copy(buf, v)
			return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(bufSize), 0, SQLLEN(len(v)), nil
		}
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(bufSize), 0, SQL_NULL_DATA, nil

	case []byte:
		bufSize := size
		if bufSize == 0 {
			bufSize = defaultBinaryBufferSize
		}
		buf := make([]byte, bufSize)
		if direction == ParamInputOutput && len(v) > 0 {
			copy(buf, v)
			return buf, SQL_C_BINARY, SQL_VARBINARY, SQLULEN(bufSize), 0, SQLLEN(len(v)), nil
		}
		return buf, SQL_C_BINARY, SQL_VARBINARY, SQLULEN(bufSize), 0, SQL_NULL_DATA, nil

	case time.Time:
		ts := &SQL_TIMESTAMP_STRUCT{}
		if direction == ParamInputOutput && !v.IsZero() {
			ts.Year = SQLSMALLINT(v.Year())
			ts.Month = SQLUSMALLINT(v.Month())
			ts.Day = SQLUSMALLINT(v.Day())
			ts.Hour = SQLUSMALLINT(v.Hour())
			ts.Minute = SQLUSMALLINT(v.Minute())
			ts.Second = SQLUSMALLINT(v.Second())
			ts.Fraction = SQLUINTEGER((v.Nanosecond() / 1_000_000) * 1_000_000)
			return ts, SQL_C_TIMESTAMP, SQL_TYPE_TIMESTAMP, 23, 3, SQLLEN(unsafe.Sizeof(*ts)), nil
		}
		return ts, SQL_C_TIMESTAMP, SQL_TYPE_TIMESTAMP, 23, 3, SQL_NULL_DATA, nil

	case GUID:
		buf := make([]byte, 16)
		if direction == ParamInputOutput {
			copy(buf, v[:])
			return buf, SQL_C_GUID, SQL_GUID, 16, 0, 16, nil
		}
		return buf, SQL_C_GUID, SQL_GUID, 16, 0, SQL_NULL_DATA, nil

	default:
		// Fall back to string buffer for unknown types
		bufSize := size
		if bufSize == 0 {
			bufSize = defaultStringBufferSize
		}
		buf := make([]byte, bufSize+1)
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(bufSize), 0, SQL_NULL_DATA, nil
	}
}

// retrieveOutputParams reads values from output parameter buffers after execution
func (s *Stmt) retrieveOutputParams() []interface{} {
	if len(s.outputParams) == 0 {
		return nil
	}

	// Build result slice with values at correct indices
	maxIdx := 0
	for _, op := range s.outputParams {
		if op.index > maxIdx {
			maxIdx = op.index
		}
	}

	result := make([]interface{}, maxIdx+1)

	for _, op := range s.outputParams {
		// Check for NULL
		if op.length != nil && *op.length == SQL_NULL_DATA {
			result[op.index] = nil
			continue
		}

		result[op.index] = s.convertOutputBuffer(op)
	}

	return result
}

// convertOutputBuffer converts an output buffer to its Go type
func (s *Stmt) convertOutputBuffer(op outputParamInfo) interface{} {
	switch buf := op.buffer.(type) {
	case *byte:
		return *buf != 0 // Convert to bool

	case *int8:
		return *buf

	case *int16:
		return *buf

	case *int32:
		return *buf

	case *int64:
		return *buf

	case *float32:
		return *buf

	case *float64:
		return *buf

	case []byte:
		if op.cType == SQL_C_CHAR {
			// String - find null terminator or use length
			length := int(*op.length)
			if length < 0 {
				length = 0
			}
			if length > len(buf) {
				length = len(buf)
			}
			// Find null terminator within length
			for i := 0; i < length; i++ {
				if buf[i] == 0 {
					return string(buf[:i])
				}
			}
			return string(buf[:length])
		} else if op.cType == SQL_C_GUID {
			// GUID
			if len(buf) >= 16 {
				var g GUID
				copy(g[:], buf[:16])
				return g
			}
		}
		// Binary - return slice copy up to length
		length := int(*op.length)
		if length < 0 {
			length = 0
		}
		if length > len(buf) {
			length = len(buf)
		}
		result := make([]byte, length)
		copy(result, buf[:length])
		return result

	case *SQL_TIMESTAMP_STRUCT:
		return time.Date(
			int(buf.Year),
			time.Month(buf.Month),
			int(buf.Day),
			int(buf.Hour),
			int(buf.Minute),
			int(buf.Second),
			int(buf.Fraction),
			time.UTC,
		)

	default:
		return nil
	}
}

// =============================================================================
// Batch Operations Support
// =============================================================================

// ExecBatch executes a prepared statement with multiple parameter sets in a single batch.
// This uses ODBC array binding (SQL_ATTR_PARAMSET_SIZE) for efficient bulk operations.
// Returns a BatchResult with per-row status information.
func (s *Stmt) ExecBatch(ctx context.Context, paramSets [][]driver.NamedValue) (*BatchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, driver.ErrBadConn
	}

	if len(paramSets) == 0 {
		return &BatchResult{}, nil
	}

	numRows := len(paramSets)
	numParams := 0
	if len(paramSets) > 0 {
		numParams = len(paramSets[0])
	}

	result := &BatchResult{
		RowCounts: make([]int64, numRows),
		Errors:    make([]error, numRows),
	}

	// Try to use true array binding
	arrayBindingWorked := s.execBatchArrayBinding(ctx, paramSets, numRows, numParams, result)

	if !arrayBindingWorked {
		// Fall back to row-by-row execution if array binding fails
		s.execBatchRowByRow(ctx, paramSets, result)
	}

	s.outputParams = nil

	return result, nil
}

// execBatchArrayBinding attempts to use ODBC array binding for batch execution
// Returns true if array binding was successful, false if fallback is needed
func (s *Stmt) execBatchArrayBinding(ctx context.Context, paramSets [][]driver.NamedValue, numRows, numParams int, result *BatchResult) bool {
	if numParams == 0 {
		return false
	}

	// Set up array binding
	// Set paramset size
	ret := SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, uintptr(numRows), 0)
	if !IsSuccess(ret) {
		return false // Driver doesn't support array binding
	}

	// Set column-wise binding
	ret = SetStmtAttr(s.stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0)
	if !IsSuccess(ret) {
		// Reset paramset size and fall back
		SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
		return false
	}

	// Allocate status array for per-row results
	statusArray := make([]SQLUSMALLINT, numRows)
	ret = SetStmtAttr(s.stmt, SQL_ATTR_PARAM_STATUS_PTR, uintptr(unsafe.Pointer(&statusArray[0])), 0)
	if !IsSuccess(ret) {
		SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
		return false
	}

	// Track number of rows processed
	var rowsProcessed SQLULEN
	ret = SetStmtAttr(s.stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, uintptr(unsafe.Pointer(&rowsProcessed)), 0)
	if !IsSuccess(ret) {
		SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
		return false
	}

	// Allocate column buffers for each parameter
	columnBuffers := make([]*ColumnBuffer, numParams)

	for paramIdx := 0; paramIdx < numParams; paramIdx++ {
		// Collect all values for this column
		values := make([]interface{}, numRows)
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if paramIdx < len(paramSets[rowIdx]) {
				values[rowIdx] = paramSets[rowIdx][paramIdx].Value
			}
		}

		// Allocate the column buffer
		colBuf, err := AllocateColumnArray(values, numRows)
		if err != nil || colBuf == nil {
			// Reset and fall back
			SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
			FreeStmt(s.stmt, SQL_RESET_PARAMS)
			return false
		}
		columnBuffers[paramIdx] = colBuf

		// Bind the parameter array
		dataPtr := colBuf.GetColumnBufferPtr()
		ret = BindParameter(
			s.stmt,
			SQLUSMALLINT(paramIdx+1),
			SQL_PARAM_INPUT,
			colBuf.CType,
			colBuf.SQLType,
			colBuf.ColSize,
			colBuf.DecDigits,
			dataPtr,
			SQLLEN(colBuf.ElemSize),
			&colBuf.Lengths[0],
		)
		if !IsSuccess(ret) {
			SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
			FreeStmt(s.stmt, SQL_RESET_PARAMS)
			return false
		}
	}

	// Execute the batch
	ret = Execute(s.stmt)

	// Process results
	if IsSuccess(ret) || ret == SQL_SUCCESS_WITH_INFO {
		// Get rows affected
		var totalRowCount SQLLEN
		RowCount(s.stmt, &totalRowCount)
		result.TotalRowsAffected = int64(totalRowCount)

		// Distribute row counts based on status
		successCount := 0
		for i := 0; i < numRows; i++ {
			switch statusArray[i] {
			case SQL_PARAM_SUCCESS, SQL_PARAM_SUCCESS_WITH_INFO:
				// For simplicity, assume 1 row affected per successful param set
				result.RowCounts[i] = 1
				successCount++
			case SQL_PARAM_ERROR:
				result.Errors[i] = fmt.Errorf("batch row %d failed", i)
			case SQL_PARAM_UNUSED:
				// Row was not processed
				result.RowCounts[i] = 0
			default:
				// DIAG_UNAVAILABLE or unknown
				result.RowCounts[i] = 1
				successCount++
			}
		}
		// Adjust total if we got actual count
		if successCount > 0 && result.TotalRowsAffected > 0 {
			avgPerRow := result.TotalRowsAffected / int64(successCount)
			for i := 0; i < numRows; i++ {
				if result.Errors[i] == nil && result.RowCounts[i] > 0 {
					result.RowCounts[i] = avgPerRow
				}
			}
		}
	} else if ret == SQL_NO_DATA {
		// No rows affected
		result.TotalRowsAffected = 0
	} else {
		// Batch failed entirely
		err := NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
		for i := 0; i < numRows; i++ {
			result.Errors[i] = err
		}
	}

	// Reset for normal operation
	SetStmtAttr(s.stmt, SQL_ATTR_PARAMSET_SIZE, 1, 0)
	FreeStmt(s.stmt, SQL_RESET_PARAMS)

	return true
}

// execBatchRowByRow executes each parameter set individually (fallback)
func (s *Stmt) execBatchRowByRow(ctx context.Context, paramSets [][]driver.NamedValue, result *BatchResult) {
	for i, params := range paramSets {
		// Clear and bind parameters for this set
		s.paramBuffers = make([]interface{}, len(params))
		s.paramLengths = make([]SQLLEN, len(params))
		s.outputParams = nil

		for _, param := range params {
			paramNum := SQLUSMALLINT(param.Ordinal)
			if paramNum == 0 {
				continue
			}
			if err := s.bindParam(paramNum, param.Value); err != nil {
				result.Errors[i] = err
				continue
			}
		}

		if result.Errors[i] != nil {
			continue
		}

		// Execute
		ret := Execute(s.stmt)
		if !IsSuccess(ret) && ret != SQL_NO_DATA {
			result.Errors[i] = NewError(SQL_HANDLE_STMT, SQLHANDLE(s.stmt))
			continue
		}

		// Get rows affected
		var rowCount SQLLEN
		RowCount(s.stmt, &rowCount)
		result.RowCounts[i] = int64(rowCount)
		result.TotalRowsAffected += int64(rowCount)

		// Reset parameters for next set
		FreeStmt(s.stmt, SQL_RESET_PARAMS)
	}
}

// isInsertStatement checks if a SQL statement is an INSERT statement
func isInsertStatement(query string) bool {
	// Skip leading whitespace and find the first non-whitespace character
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		// Check if the statement starts with INSERT (case-insensitive)
		remaining := query[i:]
		if len(remaining) >= 6 {
			prefix := remaining[:6]
			if (prefix[0] == 'I' || prefix[0] == 'i') &&
				(prefix[1] == 'N' || prefix[1] == 'n') &&
				(prefix[2] == 'S' || prefix[2] == 's') &&
				(prefix[3] == 'E' || prefix[3] == 'e') &&
				(prefix[4] == 'R' || prefix[4] == 'r') &&
				(prefix[5] == 'T' || prefix[5] == 't') {
				// Ensure the next character is whitespace or end of string
				if len(remaining) == 6 || remaining[6] == ' ' || remaining[6] == '\t' || remaining[6] == '\n' || remaining[6] == '\r' {
					return true
				}
			}
		}
		return false
	}
	return false
}

// Ensure Stmt implements the required interfaces
var (
	_ driver.Stmt             = (*Stmt)(nil)
	_ driver.StmtExecContext  = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
)
