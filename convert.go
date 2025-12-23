package odbc

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// convertToODBC converts a Go value to ODBC binding parameters
// Returns: buffer, C type, SQL type, column size, decimal digits, length indicator, error
func convertToODBC(value interface{}) (interface{}, SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLLEN, error) {
	if value == nil {
		return nil, SQL_C_CHAR, SQL_VARCHAR, 0, 0, SQLLEN(SQL_NULL_DATA), nil
	}

	switch v := value.(type) {
	case bool:
		b := new(byte)
		if v {
			*b = 1
		}
		return b, SQL_C_BIT, SQL_BIT, 1, 0, 1, nil

	case int:
		val := new(int64)
		*val = int64(v)
		return val, SQL_C_SBIGINT, SQL_BIGINT, 20, 0, 8, nil

	case int8:
		val := new(int8)
		*val = v
		return val, SQL_C_STINYINT, SQL_TINYINT, 4, 0, 1, nil

	case int16:
		val := new(int16)
		*val = v
		return val, SQL_C_SSHORT, SQL_SMALLINT, 6, 0, 2, nil

	case int32:
		val := new(int32)
		*val = v
		return val, SQL_C_SLONG, SQL_INTEGER, 11, 0, 4, nil

	case int64:
		val := new(int64)
		*val = v
		return val, SQL_C_SBIGINT, SQL_BIGINT, 20, 0, 8, nil

	case uint:
		val := new(int64)
		*val = int64(v)
		return val, SQL_C_SBIGINT, SQL_BIGINT, 20, 0, 8, nil

	case uint8:
		val := new(uint8)
		*val = v
		return val, SQL_C_UTINYINT, SQL_TINYINT, 3, 0, 1, nil

	case uint16:
		val := new(uint16)
		*val = v
		return val, SQL_C_USHORT, SQL_SMALLINT, 5, 0, 2, nil

	case uint32:
		val := new(uint32)
		*val = v
		return val, SQL_C_ULONG, SQL_INTEGER, 10, 0, 4, nil

	case uint64:
		// Convert to string for large uint64 values to avoid overflow
		s := strconv.FormatUint(v, 10)
		buf := append([]byte(s), 0)
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(len(s)), 0, SQLLEN(len(s)), nil

	case float32:
		val := new(float32)
		*val = v
		return val, SQL_C_FLOAT, SQL_REAL, 7, 0, 4, nil

	case float64:
		val := new(float64)
		*val = v
		return val, SQL_C_DOUBLE, SQL_DOUBLE, 15, 0, 8, nil

	case string:
		buf := append([]byte(v), 0) // Null-terminated
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(len(v)), 0, SQLLEN(len(v)), nil

	case []byte:
		if len(v) == 0 {
			return nil, SQL_C_BINARY, SQL_VARBINARY, 0, 0, 0, nil
		}
		return v, SQL_C_BINARY, SQL_VARBINARY, SQLULEN(len(v)), 0, SQLLEN(len(v)), nil

	case time.Time:
		// Convert nanoseconds to billionths, but truncate to milliseconds (3 decimal places)
		// for broader database compatibility (SQL Server DATETIME only supports ~3.33ms precision)
		// Fraction field is in billionths of a second (nanoseconds)
		// To get millisecond precision: (nanoseconds / 1_000_000) * 1_000_000
		fraction := SQLUINTEGER((v.Nanosecond() / 1_000_000) * 1_000_000)
		ts := &SQL_TIMESTAMP_STRUCT{
			Year:     SQLSMALLINT(v.Year()),
			Month:    SQLUSMALLINT(v.Month()),
			Day:      SQLUSMALLINT(v.Day()),
			Hour:     SQLUSMALLINT(v.Hour()),
			Minute:   SQLUSMALLINT(v.Minute()),
			Second:   SQLUSMALLINT(v.Second()),
			Fraction: fraction,
		}
		// Use column size 23 and decimal digits 3 for broader compatibility
		// This matches SQL Server datetime2(3) precision
		return ts, SQL_C_TIMESTAMP, SQL_TYPE_TIMESTAMP, 23, 3, SQLLEN(unsafe.Sizeof(*ts)), nil

	default:
		// Try to convert to string
		s := fmt.Sprintf("%v", v)
		buf := append([]byte(s), 0)
		return buf, SQL_C_CHAR, SQL_VARCHAR, SQLULEN(len(s)), 0, SQLLEN(len(s)), nil
	}
}

// getBufferPtr returns a pointer to the buffer data and its length
func getBufferPtr(buf interface{}) (uintptr, SQLLEN) {
	switch v := buf.(type) {
	case []byte:
		if len(v) == 0 {
			return 0, 0
		}
		return uintptr(unsafe.Pointer(&v[0])), SQLLEN(len(v))

	case *int8:
		return uintptr(unsafe.Pointer(v)), 1

	case *int16:
		return uintptr(unsafe.Pointer(v)), 2

	case *int32:
		return uintptr(unsafe.Pointer(v)), 4

	case *int64:
		return uintptr(unsafe.Pointer(v)), 8

	case *uint8: // same as *byte
		return uintptr(unsafe.Pointer(v)), 1

	case *uint16:
		return uintptr(unsafe.Pointer(v)), 2

	case *uint32:
		return uintptr(unsafe.Pointer(v)), 4

	case *uint64:
		return uintptr(unsafe.Pointer(v)), 8

	case *float32:
		return uintptr(unsafe.Pointer(v)), 4

	case *float64:
		return uintptr(unsafe.Pointer(v)), 8

	case *SQL_TIMESTAMP_STRUCT:
		return uintptr(unsafe.Pointer(v)), SQLLEN(unsafe.Sizeof(*v))

	case *SQL_DATE_STRUCT:
		return uintptr(unsafe.Pointer(v)), SQLLEN(unsafe.Sizeof(*v))

	case *SQL_TIME_STRUCT:
		return uintptr(unsafe.Pointer(v)), SQLLEN(unsafe.Sizeof(*v))

	default:
		return 0, 0
	}
}

// SQLTypeName returns a human-readable name for an SQL type
func SQLTypeName(sqlType SQLSMALLINT) string {
	switch sqlType {
	case SQL_CHAR:
		return "CHAR"
	case SQL_VARCHAR:
		return "VARCHAR"
	case SQL_LONGVARCHAR:
		return "LONGVARCHAR"
	case SQL_WCHAR:
		return "WCHAR"
	case SQL_WVARCHAR:
		return "WVARCHAR"
	case SQL_WLONGVARCHAR:
		return "WLONGVARCHAR"
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
		return "LONGVARBINARY"
	case SQL_TYPE_DATE:
		return "DATE"
	case SQL_TYPE_TIME:
		return "TIME"
	case SQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case SQL_DATETIME:
		return "DATETIME"
	case SQL_GUID:
		return "GUID"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", sqlType)
	}
}
