package odbc

import (
	"database/sql/driver"
	"io"
	"reflect"
	"time"
	"unsafe"
)

// Rows implements driver.Rows for result set iteration
type Rows struct {
	stmt      *Stmt
	columns   []string
	colTypes  []SQLSMALLINT
	colSizes  []SQLULEN
	decDigits []SQLSMALLINT // decimal digits (scale) for NUMERIC/DECIMAL types
	nullable  []SQLSMALLINT
	closed    bool
	closeStmt bool // Whether to close the statement when rows are closed
}

// newRows creates a new Rows from a statement
func newRows(stmt *Stmt, closeStmt bool) (*Rows, error) {
	var numCols SQLSMALLINT
	ret := NumResultCols(stmt.stmt, &numCols)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(stmt.stmt))
	}

	if numCols == 0 {
		// No result set (e.g., UPDATE/INSERT)
		return &Rows{
			stmt:      stmt,
			columns:   nil,
			closeStmt: closeStmt,
		}, nil
	}

	columns := make([]string, numCols)
	colTypes := make([]SQLSMALLINT, numCols)
	colSizes := make([]SQLULEN, numCols)
	decDigits := make([]SQLSMALLINT, numCols)
	nullable := make([]SQLSMALLINT, numCols)

	colName := make([]byte, 256)
	for i := SQLUSMALLINT(1); i <= SQLUSMALLINT(numCols); i++ {
		nameLen, dataType, colSize, decDigitsVal, nullableVal, ret := DescribeCol(stmt.stmt, i, colName)
		if !IsSuccess(ret) {
			return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(stmt.stmt))
		}

		columns[i-1] = string(colName[:nameLen])
		colTypes[i-1] = dataType
		colSizes[i-1] = colSize
		decDigits[i-1] = decDigitsVal
		nullable[i-1] = nullableVal
	}

	return &Rows{
		stmt:      stmt,
		columns:   columns,
		colTypes:  colTypes,
		colSizes:  colSizes,
		decDigits: decDigits,
		nullable:  nullable,
		closeStmt: closeStmt,
	}, nil
}

// Columns returns the column names
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Close cursor
	CloseCursor(r.stmt.stmt)

	// Close statement if we own it
	if r.closeStmt && r.stmt != nil {
		return r.stmt.Close()
	}

	return nil
}

// Next fetches the next row
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	ret := Fetch(r.stmt.stmt)
	if ret == SQL_NO_DATA {
		return io.EOF
	}
	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}

	// Get data for each column
	for i := 0; i < len(dest); i++ {
		val, err := r.getColumnData(SQLUSMALLINT(i + 1))
		if err != nil {
			return err
		}
		dest[i] = val
	}

	return nil
}

// getColumnData retrieves data for a single column
func (r *Rows) getColumnData(colNum SQLUSMALLINT) (interface{}, error) {
	idx := int(colNum) - 1
	if idx < 0 || idx >= len(r.colTypes) {
		return nil, nil
	}

	colType := r.colTypes[idx]
	colSize := r.colSizes[idx]

	switch colType {
	case SQL_BIT:
		return r.getBool(colNum)
	case SQL_TINYINT:
		return r.getInt8(colNum)
	case SQL_SMALLINT:
		return r.getInt16(colNum)
	case SQL_INTEGER:
		return r.getInt32(colNum)
	case SQL_BIGINT:
		return r.getInt64(colNum)
	case SQL_REAL:
		return r.getFloat32(colNum)
	case SQL_FLOAT, SQL_DOUBLE:
		return r.getFloat64(colNum)
	case SQL_NUMERIC, SQL_DECIMAL:
		// Get as string and parse
		return r.getString(colNum, colSize)
	case SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR:
		return r.getString(colNum, colSize)
	case SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR:
		return r.getWideString(colNum, colSize)
	case SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY:
		return r.getBytes(colNum, colSize)
	case SQL_TYPE_DATE:
		return r.getDate(colNum)
	case SQL_TYPE_TIME:
		return r.getTime(colNum)
	case SQL_TYPE_TIMESTAMP, SQL_DATETIME:
		return r.getTimestamp(colNum)
	case SQL_GUID:
		return r.getGUID(colNum)
	default:
		// Default to string
		return r.getString(colNum, colSize)
	}
}

func (r *Rows) getBool(colNum SQLUSMALLINT) (interface{}, error) {
	var value byte
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_BIT, uintptr(unsafe.Pointer(&value)), 1, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return value != 0, nil
}

func (r *Rows) getInt8(colNum SQLUSMALLINT) (interface{}, error) {
	var value int8
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_STINYINT, uintptr(unsafe.Pointer(&value)), 1, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return int64(value), nil
}

func (r *Rows) getInt16(colNum SQLUSMALLINT) (interface{}, error) {
	var value int16
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_SSHORT, uintptr(unsafe.Pointer(&value)), 2, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return int64(value), nil
}

func (r *Rows) getInt32(colNum SQLUSMALLINT) (interface{}, error) {
	var value int32
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_SLONG, uintptr(unsafe.Pointer(&value)), 4, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return int64(value), nil
}

func (r *Rows) getInt64(colNum SQLUSMALLINT) (interface{}, error) {
	var value int64
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_SBIGINT, uintptr(unsafe.Pointer(&value)), 8, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return value, nil
}

func (r *Rows) getFloat32(colNum SQLUSMALLINT) (interface{}, error) {
	var value float32
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_FLOAT, uintptr(unsafe.Pointer(&value)), 4, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return float64(value), nil
}

func (r *Rows) getFloat64(colNum SQLUSMALLINT) (interface{}, error) {
	var value float64
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_DOUBLE, uintptr(unsafe.Pointer(&value)), 8, &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return value, nil
}

func (r *Rows) getString(colNum SQLUSMALLINT, colSize SQLULEN) (interface{}, error) {
	// Start with a reasonable buffer size
	bufSize := int(colSize) + 1
	if bufSize < 256 {
		bufSize = 256
	}
	if bufSize > 65536 {
		bufSize = 65536 // Cap initial buffer
	}

	buf := make([]byte, bufSize)
	var indicator SQLLEN

	ret := GetData(r.stmt.stmt, colNum, SQL_C_CHAR, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(len(buf)), &indicator)
	if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}

	// Handle data truncation - need larger buffer
	if ret == SQL_SUCCESS_WITH_INFO && indicator > SQLLEN(len(buf)-1) {
		// Reallocate and fetch remaining data
		totalLen := int(indicator)
		result := make([]byte, 0, totalLen)
		result = append(result, buf[:len(buf)-1]...) // Already fetched (minus null terminator)

		remaining := totalLen - (len(buf) - 1)
		for remaining > 0 {
			chunkSize := remaining + 1
			if chunkSize > len(buf) {
				chunkSize = len(buf)
			}
			ret = GetData(r.stmt.stmt, colNum, SQL_C_CHAR, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(chunkSize), &indicator)
			if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
				break
			}
			if ret == SQL_NO_DATA || indicator == SQLLEN(SQL_NULL_DATA) {
				break
			}
			copyLen := int(indicator)
			if copyLen > chunkSize-1 {
				copyLen = chunkSize - 1
			}
			result = append(result, buf[:copyLen]...)
			remaining -= copyLen
		}
		return string(result), nil
	}

	// Normal case - data fit in buffer
	if indicator >= 0 && int(indicator) < len(buf) {
		return string(buf[:indicator]), nil
	}
	// Find null terminator
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i]), nil
		}
	}
	return string(buf), nil
}

func (r *Rows) getBytes(colNum SQLUSMALLINT, colSize SQLULEN) (interface{}, error) {
	// Start with a reasonable buffer size
	bufSize := int(colSize)
	if bufSize < 256 {
		bufSize = 256
	}
	if bufSize > 65536 {
		bufSize = 65536 // Cap initial buffer
	}

	buf := make([]byte, bufSize)
	var indicator SQLLEN

	ret := GetData(r.stmt.stmt, colNum, SQL_C_BINARY, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(len(buf)), &indicator)
	if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}

	// Handle data truncation
	if ret == SQL_SUCCESS_WITH_INFO && indicator > SQLLEN(len(buf)) {
		totalLen := int(indicator)
		result := make([]byte, 0, totalLen)
		result = append(result, buf...)

		remaining := totalLen - len(buf)
		for remaining > 0 {
			chunkSize := remaining
			if chunkSize > len(buf) {
				chunkSize = len(buf)
			}
			ret = GetData(r.stmt.stmt, colNum, SQL_C_BINARY, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(chunkSize), &indicator)
			if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
				break
			}
			if ret == SQL_NO_DATA || indicator == SQLLEN(SQL_NULL_DATA) {
				break
			}
			copyLen := int(indicator)
			if copyLen > chunkSize {
				copyLen = chunkSize
			}
			result = append(result, buf[:copyLen]...)
			remaining -= copyLen
		}
		return result, nil
	}

	if indicator >= 0 && int(indicator) <= len(buf) {
		return buf[:indicator], nil
	}
	return buf, nil
}

func (r *Rows) getDate(colNum SQLUSMALLINT) (interface{}, error) {
	var date SQL_DATE_STRUCT
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_DATE, uintptr(unsafe.Pointer(&date)), SQLLEN(unsafe.Sizeof(date)), &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return time.Date(int(date.Year), time.Month(date.Month), int(date.Day), 0, 0, 0, 0, time.UTC), nil
}

func (r *Rows) getTime(colNum SQLUSMALLINT) (interface{}, error) {
	var t SQL_TIME_STRUCT
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_TIME, uintptr(unsafe.Pointer(&t)), SQLLEN(unsafe.Sizeof(t)), &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return time.Date(0, 1, 1, int(t.Hour), int(t.Minute), int(t.Second), 0, time.UTC), nil
}

func (r *Rows) getTimestamp(colNum SQLUSMALLINT) (interface{}, error) {
	var ts SQL_TIMESTAMP_STRUCT
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_TIMESTAMP, uintptr(unsafe.Pointer(&ts)), SQLLEN(unsafe.Sizeof(ts)), &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	// Fraction is in billionths of a second, convert to nanoseconds
	nanos := int(ts.Fraction)
	return time.Date(int(ts.Year), time.Month(ts.Month), int(ts.Day),
		int(ts.Hour), int(ts.Minute), int(ts.Second), nanos, time.UTC), nil
}

// getWideString retrieves a wide character (UTF-16) string and converts to UTF-8
func (r *Rows) getWideString(colNum SQLUSMALLINT, colSize SQLULEN) (interface{}, error) {
	// Buffer size in UTF-16 code units (2 bytes each)
	bufSize := int(colSize) + 1
	if bufSize < 256 {
		bufSize = 256
	}
	if bufSize > 32768 {
		bufSize = 32768 // Cap initial buffer (in code units)
	}

	// Allocate buffer for UTF-16 data (2 bytes per code unit)
	buf := make([]uint16, bufSize)
	var indicator SQLLEN

	ret := GetData(r.stmt.stmt, colNum, SQL_C_WCHAR, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(len(buf)*2), &indicator)
	if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}

	// Handle data truncation - need larger buffer
	if ret == SQL_SUCCESS_WITH_INFO && indicator > SQLLEN((len(buf)-1)*2) {
		// Reallocate and fetch remaining data
		totalBytes := int(indicator)
		totalUnits := totalBytes / 2
		result := make([]uint16, 0, totalUnits)
		// Already fetched (minus null terminator)
		fetchedUnits := len(buf) - 1
		result = append(result, buf[:fetchedUnits]...)

		remaining := totalUnits - fetchedUnits
		for remaining > 0 {
			chunkUnits := remaining + 1
			if chunkUnits > len(buf) {
				chunkUnits = len(buf)
			}
			ret = GetData(r.stmt.stmt, colNum, SQL_C_WCHAR, uintptr(unsafe.Pointer(&buf[0])), SQLLEN(chunkUnits*2), &indicator)
			if !IsSuccess(ret) && ret != SQL_SUCCESS_WITH_INFO {
				break
			}
			if ret == SQL_NO_DATA || indicator == SQLLEN(SQL_NULL_DATA) {
				break
			}
			copyUnits := int(indicator) / 2
			if copyUnits > chunkUnits-1 {
				copyUnits = chunkUnits - 1
			}
			result = append(result, buf[:copyUnits]...)
			remaining -= copyUnits
		}
		return utf16ToString(result), nil
	}

	// Normal case - data fit in buffer
	if indicator >= 0 {
		numUnits := int(indicator) / 2
		if numUnits > len(buf)-1 {
			numUnits = len(buf) - 1
		}
		return utf16ToString(buf[:numUnits]), nil
	}
	// Find null terminator
	for i, c := range buf {
		if c == 0 {
			return utf16ToString(buf[:i]), nil
		}
	}
	return utf16ToString(buf), nil
}

// utf16ToString converts a UTF-16 encoded slice to a UTF-8 string
func utf16ToString(u []uint16) string {
	// Convert UTF-16 to runes, then to string
	runes := make([]rune, 0, len(u))
	for i := 0; i < len(u); i++ {
		r := u[i]
		if r >= 0xD800 && r <= 0xDBFF && i+1 < len(u) {
			// High surrogate - check for low surrogate
			r2 := u[i+1]
			if r2 >= 0xDC00 && r2 <= 0xDFFF {
				// Valid surrogate pair - decode to rune
				runes = append(runes, rune(((int(r)-0xD800)<<10)+(int(r2)-0xDC00)+0x10000))
				i++
				continue
			}
		}
		runes = append(runes, rune(r))
	}
	return string(runes)
}

// getGUID retrieves a GUID value as a formatted string
func (r *Rows) getGUID(colNum SQLUSMALLINT) (interface{}, error) {
	var guid SQL_GUID_STRUCT
	var indicator SQLLEN
	ret := GetData(r.stmt.stmt, colNum, SQL_C_GUID, uintptr(unsafe.Pointer(&guid)), SQLLEN(unsafe.Sizeof(guid)), &indicator)
	if !IsSuccess(ret) {
		return nil, NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}
	if indicator == SQLLEN(SQL_NULL_DATA) {
		return nil, nil
	}
	return guid.String(), nil
}

// ColumnTypeScanType returns the Go type suitable for scanning into
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.colTypes) {
		return reflect.TypeOf(new(interface{})).Elem()
	}

	switch r.colTypes[index] {
	case SQL_BIT:
		return reflect.TypeOf(false)
	case SQL_TINYINT, SQL_SMALLINT, SQL_INTEGER, SQL_BIGINT:
		return reflect.TypeOf(int64(0))
	case SQL_REAL:
		return reflect.TypeOf(float32(0))
	case SQL_FLOAT, SQL_DOUBLE:
		return reflect.TypeOf(float64(0))
	case SQL_NUMERIC, SQL_DECIMAL:
		return reflect.TypeOf("") // String preserves decimal precision
	case SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR, SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR:
		return reflect.TypeOf("")
	case SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY:
		return reflect.TypeOf([]byte{})
	case SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, SQL_DATETIME:
		return reflect.TypeOf(time.Time{})
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

// ColumnTypeDatabaseTypeName returns the database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.colTypes) {
		return ""
	}

	switch r.colTypes[index] {
	case SQL_CHAR:
		return "CHAR"
	case SQL_VARCHAR:
		return "VARCHAR"
	case SQL_LONGVARCHAR:
		return "TEXT"
	case SQL_WCHAR:
		return "NCHAR"
	case SQL_WVARCHAR:
		return "NVARCHAR"
	case SQL_WLONGVARCHAR:
		return "NTEXT"
	case SQL_DECIMAL:
		return "DECIMAL"
	case SQL_NUMERIC:
		return "NUMERIC"
	case SQL_SMALLINT:
		return "SMALLINT"
	case SQL_INTEGER:
		return "INTEGER"
	case SQL_REAL:
		return "REAL"
	case SQL_FLOAT:
		return "FLOAT"
	case SQL_DOUBLE:
		return "DOUBLE"
	case SQL_BIT:
		return "BIT"
	case SQL_TINYINT:
		return "TINYINT"
	case SQL_BIGINT:
		return "BIGINT"
	case SQL_BINARY:
		return "BINARY"
	case SQL_VARBINARY:
		return "VARBINARY"
	case SQL_LONGVARBINARY:
		return "BLOB"
	case SQL_TYPE_DATE:
		return "DATE"
	case SQL_TYPE_TIME:
		return "TIME"
	case SQL_TYPE_TIMESTAMP, SQL_DATETIME:
		return "TIMESTAMP"
	case SQL_GUID:
		return "GUID"
	default:
		return "UNKNOWN"
	}
}

// ColumnTypeLength returns the length of a column
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.colSizes) {
		return 0, false
	}
	// Only return length for variable-length types
	switch r.colTypes[index] {
	case SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR, SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR,
		SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY:
		return int64(r.colSizes[index]), true
	}
	return 0, false
}

// ColumnTypeNullable returns whether a column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.nullable) {
		return false, false
	}
	switch r.nullable[index] {
	case SQL_NO_NULLS:
		return false, true
	case SQL_NULLABLE:
		return true, true
	default:
		return false, false // Unknown
	}
}

// ColumnTypePrecisionScale returns the precision and scale for NUMERIC/DECIMAL types
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.colTypes) {
		return 0, 0, false
	}
	switch r.colTypes[index] {
	case SQL_NUMERIC, SQL_DECIMAL:
		// colSize = precision (total digits), decDigits = scale (digits after decimal)
		return int64(r.colSizes[index]), int64(r.decDigits[index]), true
	default:
		return 0, 0, false
	}
}

// HasNextResultSet checks if there are more result sets
func (r *Rows) HasNextResultSet() bool {
	return MoreResults(r.stmt.stmt) == SQL_SUCCESS
}

// NextResultSet advances to the next result set
func (r *Rows) NextResultSet() error {
	ret := MoreResults(r.stmt.stmt)
	if ret == SQL_NO_DATA {
		return io.EOF
	}
	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}

	// Re-fetch column info for new result set
	var numCols SQLSMALLINT
	ret = NumResultCols(r.stmt.stmt, &numCols)
	if !IsSuccess(ret) {
		return NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
	}

	columns := make([]string, numCols)
	colTypes := make([]SQLSMALLINT, numCols)
	colSizes := make([]SQLULEN, numCols)
	decDigits := make([]SQLSMALLINT, numCols)
	nullable := make([]SQLSMALLINT, numCols)

	colName := make([]byte, 256)
	for i := SQLUSMALLINT(1); i <= SQLUSMALLINT(numCols); i++ {
		nameLen, dataType, colSize, decDigitsVal, nullableVal, ret := DescribeCol(r.stmt.stmt, i, colName)
		if !IsSuccess(ret) {
			return NewError(SQL_HANDLE_STMT, SQLHANDLE(r.stmt.stmt))
		}

		columns[i-1] = string(colName[:nameLen])
		colTypes[i-1] = dataType
		colSizes[i-1] = colSize
		decDigits[i-1] = decDigitsVal
		nullable[i-1] = nullableVal
	}

	r.columns = columns
	r.colTypes = colTypes
	r.colSizes = colSizes
	r.decDigits = decDigits
	r.nullable = nullable

	return nil
}

// Ensure Rows implements the required interfaces
var (
	_ driver.Rows                           = (*Rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*Rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
	_ driver.RowsColumnTypeLength           = (*Rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*Rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*Rows)(nil)
	_ driver.RowsNextResultSet              = (*Rows)(nil)
)
