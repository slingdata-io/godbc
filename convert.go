package odbc

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// GUID represents a UUID/GUID value for use as a parameter
type GUID [16]byte

// =============================================================================
// Timestamp Precision Helpers
// =============================================================================

// truncateFraction truncates nanoseconds to the specified precision
func truncateFraction(nanos int, precision TimestampPrecision) SQLUINTEGER {
	switch precision {
	case TimestampPrecisionSeconds:
		return 0
	case TimestampPrecisionMilliseconds:
		return SQLUINTEGER((nanos / 1_000_000) * 1_000_000)
	case TimestampPrecisionMicroseconds:
		return SQLUINTEGER((nanos / 1_000) * 1_000)
	case TimestampPrecisionNanoseconds:
		return SQLUINTEGER(nanos)
	default:
		// Default to milliseconds for backward compatibility
		return SQLUINTEGER((nanos / 1_000_000) * 1_000_000)
	}
}

// timestampColumnSize returns the ODBC column size for a given precision
// Format: YYYY-MM-DD HH:MM:SS[.fractional]
// Base size: 19 (no fractional), with fractional: 20 + precision
func timestampColumnSize(precision TimestampPrecision) SQLULEN {
	if precision == 0 {
		return 19
	}
	return SQLULEN(20 + int(precision))
}

// =============================================================================
// UTF-16 Conversion Helpers
// =============================================================================

// stringToUTF16 converts a UTF-8 string to UTF-16LE with null terminator
func stringToUTF16(s string) []uint16 {
	runes := []rune(s)
	result := make([]uint16, 0, len(runes)+1)
	for _, r := range runes {
		if r > 0xFFFF {
			// Encode as surrogate pair
			r -= 0x10000
			result = append(result, uint16((r>>10)+0xD800))
			result = append(result, uint16((r&0x3FF)+0xDC00))
		} else {
			result = append(result, uint16(r))
		}
	}
	result = append(result, 0) // Null terminator
	return result
}

// =============================================================================
// Interval Helpers
// =============================================================================

// boolToIntervalSign converts a boolean negative flag to ODBC interval sign
func boolToIntervalSign(negative bool) SQLSMALLINT {
	if negative {
		return 1
	}
	return 0
}

// abs returns the absolute value of an integer
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// ParseGUID parses a GUID string in the format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func ParseGUID(s string) (GUID, error) {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return GUID{}, fmt.Errorf("invalid GUID length: %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return GUID{}, fmt.Errorf("invalid GUID hex: %w", err)
	}
	var g GUID
	// GUID byte order: Data1 (4 bytes, little-endian), Data2 (2 bytes, LE), Data3 (2 bytes, LE), Data4 (8 bytes, big-endian)
	// But in the string, it's represented as: Data1-Data2-Data3-Data4[0:2]-Data4[2:8] all big-endian
	// We need to swap bytes for Data1, Data2, Data3
	g[0], g[1], g[2], g[3] = b[3], b[2], b[1], b[0] // Data1 swap
	g[4], g[5] = b[5], b[4]                         // Data2 swap
	g[6], g[7] = b[7], b[6]                         // Data3 swap
	copy(g[8:], b[8:])                              // Data4 stays as-is
	return g, nil
}

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

	case GUID:
		buf := make([]byte, 16)
		copy(buf, v[:])
		return buf, SQL_C_GUID, SQL_GUID, 16, 0, 16, nil

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

	// ==========================================================================
	// Enhanced Types
	// ==========================================================================

	case Timestamp:
		// Timestamp with explicit precision control
		fraction := truncateFraction(v.Time.Nanosecond(), v.Precision)
		ts := &SQL_TIMESTAMP_STRUCT{
			Year:     SQLSMALLINT(v.Time.Year()),
			Month:    SQLUSMALLINT(v.Time.Month()),
			Day:      SQLUSMALLINT(v.Time.Day()),
			Hour:     SQLUSMALLINT(v.Time.Hour()),
			Minute:   SQLUSMALLINT(v.Time.Minute()),
			Second:   SQLUSMALLINT(v.Time.Second()),
			Fraction: fraction,
		}
		colSize := timestampColumnSize(v.Precision)
		decDigits := SQLSMALLINT(v.Precision)
		return ts, SQL_C_TIMESTAMP, SQL_TYPE_TIMESTAMP, colSize, decDigits, SQLLEN(unsafe.Sizeof(*ts)), nil

	case TimestampTZ:
		// Timezone-aware timestamp - convert to UTC for storage
		t := v.Time
		if v.TZ != nil && v.TZ != time.UTC {
			t = t.UTC()
		}
		fraction := truncateFraction(t.Nanosecond(), v.Precision)
		ts := &SQL_TIMESTAMP_STRUCT{
			Year:     SQLSMALLINT(t.Year()),
			Month:    SQLUSMALLINT(t.Month()),
			Day:      SQLUSMALLINT(t.Day()),
			Hour:     SQLUSMALLINT(t.Hour()),
			Minute:   SQLUSMALLINT(t.Minute()),
			Second:   SQLUSMALLINT(t.Second()),
			Fraction: fraction,
		}
		colSize := timestampColumnSize(v.Precision)
		decDigits := SQLSMALLINT(v.Precision)
		return ts, SQL_C_TIMESTAMP, SQL_TYPE_TIMESTAMP, colSize, decDigits, SQLLEN(unsafe.Sizeof(*ts)), nil

	case WideString:
		// UTF-16 wide string for NVARCHAR/NCHAR columns
		utf16Buf := stringToUTF16(string(v))
		// Column size is character count (excluding null terminator)
		charCount := len(utf16Buf) - 1
		// Buffer size in bytes (2 bytes per code unit), excluding null terminator
		bufBytes := charCount * 2
		return utf16Buf, SQL_C_WCHAR, SQL_WVARCHAR, SQLULEN(charCount), 0, SQLLEN(bufBytes), nil

	case Decimal:
		// Decimal with explicit precision/scale - bind as string for maximum compatibility
		buf := append([]byte(v.Value), 0) // Null-terminated
		return buf, SQL_C_CHAR, SQL_DECIMAL, SQLULEN(v.Precision), SQLSMALLINT(v.Scale), SQLLEN(len(v.Value)), nil

	case IntervalYearMonth:
		// Year-month interval
		is := &SQL_INTERVAL_STRUCT{
			IntervalType: SQL_INTERVAL_YEAR_TO_MONTH,
			IntervalSign: boolToIntervalSign(v.Negative),
		}
		is.YearMonth.Year = SQLUINTEGER(abs(v.Years))
		is.YearMonth.Month = SQLUINTEGER(abs(v.Months))
		return is, SQL_C_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL_YEAR_TO_MONTH, 0, 0, SQLLEN(unsafe.Sizeof(*is)), nil

	case IntervalDaySecond:
		// Day-time interval
		is := &SQL_INTERVAL_STRUCT{
			IntervalType: SQL_INTERVAL_DAY_TO_SECOND,
			IntervalSign: boolToIntervalSign(v.Negative),
		}
		is.DaySecond.Day = SQLUINTEGER(abs(v.Days))
		is.DaySecond.Hour = SQLUINTEGER(abs(v.Hours))
		is.DaySecond.Minute = SQLUINTEGER(abs(v.Minutes))
		is.DaySecond.Second = SQLUINTEGER(abs(v.Seconds))
		is.DaySecond.Fraction = SQLUINTEGER(abs(v.Nanoseconds))
		return is, SQL_C_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_DAY_TO_SECOND, 0, 0, SQLLEN(unsafe.Sizeof(*is)), nil

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

	case []uint16:
		// For wide strings (UTF-16)
		if len(v) == 0 {
			return 0, 0
		}
		return uintptr(unsafe.Pointer(&v[0])), SQLLEN(len(v) * 2)

	case *SQL_INTERVAL_STRUCT:
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
	// Interval types
	case SQL_INTERVAL_YEAR:
		return "INTERVAL YEAR"
	case SQL_INTERVAL_MONTH:
		return "INTERVAL MONTH"
	case SQL_INTERVAL_DAY:
		return "INTERVAL DAY"
	case SQL_INTERVAL_HOUR:
		return "INTERVAL HOUR"
	case SQL_INTERVAL_MINUTE:
		return "INTERVAL MINUTE"
	case SQL_INTERVAL_SECOND:
		return "INTERVAL SECOND"
	case SQL_INTERVAL_YEAR_TO_MONTH:
		return "INTERVAL YEAR TO MONTH"
	case SQL_INTERVAL_DAY_TO_HOUR:
		return "INTERVAL DAY TO HOUR"
	case SQL_INTERVAL_DAY_TO_MINUTE:
		return "INTERVAL DAY TO MINUTE"
	case SQL_INTERVAL_DAY_TO_SECOND:
		return "INTERVAL DAY TO SECOND"
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		return "INTERVAL HOUR TO MINUTE"
	case SQL_INTERVAL_HOUR_TO_SECOND:
		return "INTERVAL HOUR TO SECOND"
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		return "INTERVAL MINUTE TO SECOND"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", sqlType)
	}
}
