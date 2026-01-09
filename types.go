package godbc

import "time"

// ODBC Handle types (opaque pointers)
type SQLHANDLE uintptr
type SQLHENV SQLHANDLE
type SQLHDBC SQLHANDLE
type SQLHSTMT SQLHANDLE
type SQLHDESC SQLHANDLE

// ODBC Integer types
type SQLSMALLINT int16
type SQLUSMALLINT uint16
type SQLINTEGER int32
type SQLUINTEGER uint32
type SQLLEN int64   // 64-bit for portability across platforms
type SQLULEN uint64 // 64-bit for portability across platforms
type SQLRETURN SQLSMALLINT

// ODBC Character types
type SQLCHAR byte
type SQLWCHAR uint16 // UTF-16 on Windows

// Handle type identifiers
const (
	SQL_HANDLE_ENV  SQLSMALLINT = 1
	SQL_HANDLE_DBC  SQLSMALLINT = 2
	SQL_HANDLE_STMT SQLSMALLINT = 3
	SQL_HANDLE_DESC SQLSMALLINT = 4
)

// Return codes
const (
	SQL_SUCCESS           SQLRETURN = 0
	SQL_SUCCESS_WITH_INFO SQLRETURN = 1
	SQL_ERROR             SQLRETURN = -1
	SQL_INVALID_HANDLE    SQLRETURN = -2
	SQL_NO_DATA           SQLRETURN = 100
	SQL_NEED_DATA         SQLRETURN = 99
	SQL_STILL_EXECUTING   SQLRETURN = 2
)

// Null handle constant
const SQL_NULL_HANDLE SQLHANDLE = 0

// ODBC version constants
const (
	SQL_OV_ODBC2 = 2
	SQL_OV_ODBC3 = 3
)

// Environment attributes
const (
	SQL_ATTR_ODBC_VERSION       SQLINTEGER = 200
	SQL_ATTR_CONNECTION_POOLING SQLINTEGER = 201
	SQL_ATTR_CP_MATCH           SQLINTEGER = 202
	SQL_ATTR_OUTPUT_NTS         SQLINTEGER = 10001
)

// Connection attributes
const (
	SQL_ATTR_AUTOCOMMIT      SQLINTEGER = 102
	SQL_ATTR_CONNECTION_DEAD SQLINTEGER = 1209
	SQL_ATTR_LOGIN_TIMEOUT   SQLINTEGER = 103
	SQL_ATTR_ACCESS_MODE     SQLINTEGER = 101
	SQL_ATTR_TXN_ISOLATION   SQLINTEGER = 108
)

// Autocommit values
const (
	SQL_AUTOCOMMIT_OFF = 0
	SQL_AUTOCOMMIT_ON  = 1
)

// Access mode values
const (
	SQL_MODE_READ_WRITE = 0
	SQL_MODE_READ_ONLY  = 1
)

// Transaction isolation levels
const (
	SQL_TXN_READ_UNCOMMITTED = 1
	SQL_TXN_READ_COMMITTED   = 2
	SQL_TXN_REPEATABLE_READ  = 4
	SQL_TXN_SERIALIZABLE     = 8
)

// Statement attributes
const (
	SQL_ATTR_CURSOR_TYPE        SQLINTEGER = 6
	SQL_ATTR_CONCURRENCY        SQLINTEGER = 7
	SQL_ATTR_ROW_ARRAY_SIZE     SQLINTEGER = 27
	SQL_ATTR_ROW_STATUS_PTR     SQLINTEGER = 25
	SQL_ATTR_ROWS_FETCHED       SQLINTEGER = 26
	SQL_ATTR_QUERY_TIMEOUT      SQLINTEGER = 0
	SQL_ATTR_MAX_ROWS           SQLINTEGER = 1
	SQL_ATTR_CURSOR_SCROLLABLE  SQLINTEGER = -1
	SQL_ATTR_CURSOR_SENSITIVITY SQLINTEGER = -2
)

// Cursor types
const (
	SQL_CURSOR_FORWARD_ONLY  = 0
	SQL_CURSOR_KEYSET_DRIVEN = 1
	SQL_CURSOR_DYNAMIC       = 2
	SQL_CURSOR_STATIC        = 3
)

// String terminator
const SQL_NTS SQLINTEGER = -3

// Null data indicators
const (
	SQL_NULL_DATA    SQLLEN = -1
	SQL_DATA_AT_EXEC SQLLEN = -2
)

// SQLDriverConnect options
const (
	SQL_DRIVER_NOPROMPT          SQLUSMALLINT = 0
	SQL_DRIVER_COMPLETE          SQLUSMALLINT = 1
	SQL_DRIVER_PROMPT            SQLUSMALLINT = 2
	SQL_DRIVER_COMPLETE_REQUIRED SQLUSMALLINT = 3
)

// SQL data types
const (
	SQL_UNKNOWN_TYPE   SQLSMALLINT = 0
	SQL_CHAR           SQLSMALLINT = 1
	SQL_NUMERIC        SQLSMALLINT = 2
	SQL_DECIMAL        SQLSMALLINT = 3
	SQL_INTEGER        SQLSMALLINT = 4
	SQL_SMALLINT       SQLSMALLINT = 5
	SQL_FLOAT          SQLSMALLINT = 6
	SQL_REAL           SQLSMALLINT = 7
	SQL_DOUBLE         SQLSMALLINT = 8
	SQL_DATETIME       SQLSMALLINT = 9
	SQL_VARCHAR        SQLSMALLINT = 12
	SQL_TYPE_DATE      SQLSMALLINT = 91
	SQL_TYPE_TIME      SQLSMALLINT = 92
	SQL_TYPE_TIMESTAMP SQLSMALLINT = 93
	SQL_LONGVARCHAR    SQLSMALLINT = -1
	SQL_BINARY         SQLSMALLINT = -2
	SQL_VARBINARY      SQLSMALLINT = -3
	SQL_LONGVARBINARY  SQLSMALLINT = -4
	SQL_BIGINT         SQLSMALLINT = -5
	SQL_TINYINT        SQLSMALLINT = -6
	SQL_BIT            SQLSMALLINT = -7
	SQL_BOOLEAN        SQLSMALLINT = 16 // DB2 BOOLEAN type
	SQL_WCHAR          SQLSMALLINT = -8
	SQL_WVARCHAR       SQLSMALLINT = -9
	SQL_WLONGVARCHAR   SQLSMALLINT = -10
	SQL_GUID           SQLSMALLINT = -11
)

// C data type identifiers for binding
const (
	SQL_SIGNED_OFFSET   SQLSMALLINT = -20
	SQL_UNSIGNED_OFFSET SQLSMALLINT = -22
)

const (
	SQL_C_CHAR      = SQL_CHAR
	SQL_C_LONG      = SQL_INTEGER
	SQL_C_SHORT     = SQL_SMALLINT
	SQL_C_FLOAT     = SQL_REAL
	SQL_C_DOUBLE    = SQL_DOUBLE
	SQL_C_NUMERIC   = SQL_NUMERIC
	SQL_C_DEFAULT   = 99
	SQL_C_DATE      = SQL_TYPE_DATE
	SQL_C_TIME      = SQL_TYPE_TIME
	SQL_C_TIMESTAMP = SQL_TYPE_TIMESTAMP
	SQL_C_BINARY    = SQL_BINARY
	SQL_C_BIT       = SQL_BIT
	SQL_C_WCHAR     = SQL_WCHAR
	SQL_C_SBIGINT   = SQL_BIGINT + SQL_SIGNED_OFFSET    // -25
	SQL_C_UBIGINT   = SQL_BIGINT + SQL_UNSIGNED_OFFSET  // -27
	SQL_C_SLONG     = SQL_C_LONG + SQL_SIGNED_OFFSET    // -16
	SQL_C_SSHORT    = SQL_C_SHORT + SQL_SIGNED_OFFSET   // -15
	SQL_C_STINYINT  = SQL_TINYINT + SQL_SIGNED_OFFSET   // -26
	SQL_C_ULONG     = SQL_C_LONG + SQL_UNSIGNED_OFFSET  // -18
	SQL_C_USHORT    = SQL_C_SHORT + SQL_UNSIGNED_OFFSET // -17
	SQL_C_UTINYINT  = SQL_TINYINT + SQL_UNSIGNED_OFFSET // -28
	SQL_C_GUID      = SQL_GUID
)

// Parameter input/output type
const (
	SQL_PARAM_INPUT        SQLSMALLINT = 1
	SQL_PARAM_INPUT_OUTPUT SQLSMALLINT = 2
	SQL_PARAM_OUTPUT       SQLSMALLINT = 4
)

// Fetch direction
const (
	SQL_FETCH_NEXT     SQLSMALLINT = 1
	SQL_FETCH_FIRST    SQLSMALLINT = 2
	SQL_FETCH_LAST     SQLSMALLINT = 3
	SQL_FETCH_PRIOR    SQLSMALLINT = 4
	SQL_FETCH_ABSOLUTE SQLSMALLINT = 5
	SQL_FETCH_RELATIVE SQLSMALLINT = 6
)

// Free statement options
const (
	SQL_CLOSE        SQLUSMALLINT = 0
	SQL_DROP         SQLUSMALLINT = 1
	SQL_UNBIND       SQLUSMALLINT = 2
	SQL_RESET_PARAMS SQLUSMALLINT = 3
)

// Transaction completion types
const (
	SQL_COMMIT   SQLSMALLINT = 0
	SQL_ROLLBACK SQLSMALLINT = 1
)

// Nullable field values
const (
	SQL_NO_NULLS         SQLSMALLINT = 0
	SQL_NULLABLE         SQLSMALLINT = 1
	SQL_NULLABLE_UNKNOWN SQLSMALLINT = 2
)

// Column attribute identifiers
const (
	SQL_DESC_COUNT                  SQLUSMALLINT = 1001
	SQL_DESC_TYPE                   SQLUSMALLINT = 1002
	SQL_DESC_LENGTH                 SQLUSMALLINT = 1003
	SQL_DESC_OCTET_LENGTH_PTR       SQLUSMALLINT = 1004
	SQL_DESC_PRECISION              SQLUSMALLINT = 1005
	SQL_DESC_SCALE                  SQLUSMALLINT = 1006
	SQL_DESC_DATETIME_INTERVAL_CODE SQLUSMALLINT = 1007
	SQL_DESC_NULLABLE               SQLUSMALLINT = 1008
	SQL_DESC_INDICATOR_PTR          SQLUSMALLINT = 1009
	SQL_DESC_DATA_PTR               SQLUSMALLINT = 1010
	SQL_DESC_NAME                   SQLUSMALLINT = 1011
	SQL_DESC_UNNAMED                SQLUSMALLINT = 1012
	SQL_DESC_OCTET_LENGTH           SQLUSMALLINT = 1013
	SQL_DESC_ALLOC_TYPE             SQLUSMALLINT = 1099
	SQL_DESC_CONCISE_TYPE           SQLUSMALLINT = SQL_DESC_TYPE
	SQL_DESC_DISPLAY_SIZE           SQLUSMALLINT = 6
	SQL_DESC_UNSIGNED               SQLUSMALLINT = 8
	SQL_DESC_UPDATABLE              SQLUSMALLINT = 10
	SQL_DESC_AUTO_UNIQUE_VALUE      SQLUSMALLINT = 11
	SQL_DESC_TYPE_NAME              SQLUSMALLINT = 14
	SQL_DESC_TABLE_NAME             SQLUSMALLINT = 15
	SQL_DESC_SCHEMA_NAME            SQLUSMALLINT = 16
	SQL_DESC_CATALOG_NAME           SQLUSMALLINT = 17
	SQL_DESC_BASE_COLUMN_NAME       SQLUSMALLINT = 22
	SQL_DESC_BASE_TABLE_NAME        SQLUSMALLINT = 23
	SQL_DESC_LABEL                  SQLUSMALLINT = 18
	SQL_COLUMN_LENGTH               SQLUSMALLINT = 3
	SQL_COLUMN_PRECISION            SQLUSMALLINT = 4
	SQL_COLUMN_SCALE                SQLUSMALLINT = 5
)

// SQLGetInfo information types
const (
	SQL_DRIVER_NAME           SQLUSMALLINT = 6
	SQL_DRIVER_VER            SQLUSMALLINT = 7
	SQL_DBMS_NAME             SQLUSMALLINT = 17
	SQL_DBMS_VER              SQLUSMALLINT = 18
	SQL_DATABASE_NAME         SQLUSMALLINT = 16
	SQL_SERVER_NAME           SQLUSMALLINT = 13
	SQL_USER_NAME             SQLUSMALLINT = 47
	SQL_IDENTIFIER_QUOTE_CHAR SQLUSMALLINT = 29
	SQL_MAX_IDENTIFIER_LEN    SQLUSMALLINT = 10005
)

// Timestamp struct for date/time binding
type SQL_TIMESTAMP_STRUCT struct {
	Year     SQLSMALLINT
	Month    SQLUSMALLINT
	Day      SQLUSMALLINT
	Hour     SQLUSMALLINT
	Minute   SQLUSMALLINT
	Second   SQLUSMALLINT
	Fraction SQLUINTEGER // billionths of a second
}

// Date struct
type SQL_DATE_STRUCT struct {
	Year  SQLSMALLINT
	Month SQLUSMALLINT
	Day   SQLUSMALLINT
}

// Time struct
type SQL_TIME_STRUCT struct {
	Hour   SQLUSMALLINT
	Minute SQLUSMALLINT
	Second SQLUSMALLINT
}

// Numeric struct for decimal/numeric types
type SQL_NUMERIC_STRUCT struct {
	Precision SQLCHAR
	Scale     SQLSCHAR
	Sign      SQLCHAR // 1 = positive, 0 = negative
	Val       [16]SQLCHAR
}

type SQLSCHAR int8

// GUID struct for uniqueidentifier types
type SQL_GUID_STRUCT struct {
	Data1 uint32
	Data2 uint16
	Data3 uint16
	Data4 [8]byte
}

// String returns the GUID as a formatted string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
func (g SQL_GUID_STRUCT) String() string {
	return sprintf("%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
		g.Data1, g.Data2, g.Data3,
		g.Data4[0], g.Data4[1],
		g.Data4[2], g.Data4[3], g.Data4[4], g.Data4[5], g.Data4[6], g.Data4[7])
}

// sprintf is a simple hex formatter to avoid importing fmt in types.go
func sprintf(format string, args ...interface{}) string {
	// Simple implementation for GUID formatting
	result := make([]byte, 0, 36)
	argIdx := 0
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			width := 0
			i++
			// Parse width
			for i < len(format) && format[i] >= '0' && format[i] <= '9' {
				width = width*10 + int(format[i]-'0')
				i++
			}
			if i < len(format) {
				switch format[i] {
				case 'X':
					var val uint64
					switch v := args[argIdx].(type) {
					case uint32:
						val = uint64(v)
					case uint16:
						val = uint64(v)
					case byte:
						val = uint64(v)
					}
					hex := formatHex(val, width)
					result = append(result, hex...)
					argIdx++
				}
				i++
			}
		} else {
			result = append(result, format[i])
			i++
		}
	}
	return string(result)
}

func formatHex(val uint64, width int) []byte {
	const hexDigits = "0123456789ABCDEF"
	buf := make([]byte, 16)
	pos := 15
	if val == 0 {
		buf[pos] = '0'
		pos--
	} else {
		for val > 0 {
			buf[pos] = hexDigits[val&0xF]
			val >>= 4
			pos--
		}
	}
	// Pad with zeros
	for 15-pos < width {
		buf[pos] = '0'
		pos--
	}
	return buf[pos+1:]
}

// IsSuccess checks if the return code indicates success
func IsSuccess(ret SQLRETURN) bool {
	return ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO
}

// =============================================================================
// Enhanced Type Handling Types
// =============================================================================

// TimestampPrecision specifies the fractional seconds precision for timestamps
type TimestampPrecision int

const (
	// TimestampPrecisionSeconds provides no fractional seconds (datetime)
	TimestampPrecisionSeconds TimestampPrecision = 0
	// TimestampPrecisionMilliseconds provides 3 digits (default, datetime2(3))
	TimestampPrecisionMilliseconds TimestampPrecision = 3
	// TimestampPrecisionMicroseconds provides 6 digits (datetime2(6))
	TimestampPrecisionMicroseconds TimestampPrecision = 6
	// TimestampPrecisionNanoseconds provides 9 digits (max ODBC precision)
	TimestampPrecisionNanoseconds TimestampPrecision = 9
)

// Timestamp wraps time.Time with explicit precision control
type Timestamp struct {
	Time      time.Time
	Precision TimestampPrecision
}

// NewTimestamp creates a Timestamp with the specified precision
func NewTimestamp(t time.Time, precision TimestampPrecision) Timestamp {
	return Timestamp{Time: t, Precision: precision}
}

// WideString wraps a Go string for explicit UTF-16 (NVARCHAR/NCHAR) binding.
// Use this when inserting into Unicode columns that require wide character encoding.
type WideString string

// Decimal represents a decimal value with explicit precision and scale.
// Use this for precise numeric values where floating-point approximation is unacceptable.
type Decimal struct {
	Value     string // String representation for precision preservation
	Precision int    // Total digits (1-38)
	Scale     int    // Digits after decimal point (0-Precision)
}

// NewDecimal creates a Decimal from a string with validation
func NewDecimal(value string, precision, scale int) (Decimal, error) {
	if precision < 1 || precision > 38 {
		return Decimal{}, newDecimalError("precision must be 1-38, got %d", precision)
	}
	if scale < 0 || scale > precision {
		return Decimal{}, newDecimalError("scale must be 0-%d, got %d", precision, scale)
	}
	if !isValidDecimalString(value) {
		return Decimal{}, newDecimalError("invalid decimal string: %q", value)
	}
	return Decimal{Value: value, Precision: precision, Scale: scale}, nil
}

// ParseDecimal parses a decimal string with automatic precision/scale detection
func ParseDecimal(s string) (Decimal, error) {
	if !isValidDecimalString(s) {
		return Decimal{}, newDecimalError("invalid decimal string: %q", s)
	}

	// Remove sign for counting
	val := s
	if len(val) > 0 && (val[0] == '-' || val[0] == '+') {
		val = val[1:]
	}

	// Find decimal point
	dotIdx := -1
	for i := 0; i < len(val); i++ {
		if val[i] == '.' {
			dotIdx = i
			break
		}
	}

	var precision, scale int
	if dotIdx == -1 {
		// No decimal point
		precision = len(val)
		scale = 0
	} else {
		// Has decimal point
		intPart := val[:dotIdx]
		fracPart := val[dotIdx+1:]
		precision = len(intPart) + len(fracPart)
		scale = len(fracPart)
	}

	// Handle edge cases
	if precision == 0 {
		precision = 1
	}
	if precision > 38 {
		precision = 38
	}

	return Decimal{Value: s, Precision: precision, Scale: scale}, nil
}

// isValidDecimalString validates decimal string format
func isValidDecimalString(s string) bool {
	if len(s) == 0 {
		return false
	}

	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}

	if start >= len(s) {
		return false
	}

	hasDigit := false
	hasDot := false

	for i := start; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			hasDigit = true
		} else if c == '.' {
			if hasDot {
				return false // Multiple decimal points
			}
			hasDot = true
		} else {
			return false // Invalid character
		}
	}

	return hasDigit
}

// newDecimalError creates a formatted decimal error
func newDecimalError(format string, args ...interface{}) error {
	msg := format
	if len(args) > 0 {
		// Simple formatting for common cases
		for _, arg := range args {
			switch v := arg.(type) {
			case int:
				msg = replaceFirst(msg, "%d", formatInt(v))
			case string:
				msg = replaceFirst(msg, "%q", "\""+v+"\"")
				msg = replaceFirst(msg, "%s", v)
			}
		}
	}
	return &DecimalError{Message: msg}
}

// DecimalError represents a decimal validation error
type DecimalError struct {
	Message string
}

func (e *DecimalError) Error() string {
	return "decimal: " + e.Message
}

// replaceFirst replaces the first occurrence of old with new in s
func replaceFirst(s, old, new string) string {
	for i := 0; i <= len(s)-len(old); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

// formatInt converts an int to string without importing strconv
func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// TimestampTZ represents a timestamp with timezone awareness
type TimestampTZ struct {
	Time      time.Time
	Precision TimestampPrecision
	TZ        *time.Location // nil means use connection default (UTC)
}

// NewTimestampTZ creates a timezone-aware timestamp
func NewTimestampTZ(t time.Time, precision TimestampPrecision, tz *time.Location) TimestampTZ {
	return TimestampTZ{Time: t, Precision: precision, TZ: tz}
}

// =============================================================================
// INTERVAL Types
// =============================================================================

// SQL Interval type constants
const (
	SQL_INTERVAL_YEAR             SQLSMALLINT = 101
	SQL_INTERVAL_MONTH            SQLSMALLINT = 102
	SQL_INTERVAL_DAY              SQLSMALLINT = 103
	SQL_INTERVAL_HOUR             SQLSMALLINT = 104
	SQL_INTERVAL_MINUTE           SQLSMALLINT = 105
	SQL_INTERVAL_SECOND           SQLSMALLINT = 106
	SQL_INTERVAL_YEAR_TO_MONTH    SQLSMALLINT = 107
	SQL_INTERVAL_DAY_TO_HOUR      SQLSMALLINT = 108
	SQL_INTERVAL_DAY_TO_MINUTE    SQLSMALLINT = 109
	SQL_INTERVAL_DAY_TO_SECOND    SQLSMALLINT = 110
	SQL_INTERVAL_HOUR_TO_MINUTE   SQLSMALLINT = 111
	SQL_INTERVAL_HOUR_TO_SECOND   SQLSMALLINT = 112
	SQL_INTERVAL_MINUTE_TO_SECOND SQLSMALLINT = 113
)

// C Interval type identifiers (same as SQL types for intervals)
const (
	SQL_C_INTERVAL_YEAR             = SQL_INTERVAL_YEAR
	SQL_C_INTERVAL_MONTH            = SQL_INTERVAL_MONTH
	SQL_C_INTERVAL_DAY              = SQL_INTERVAL_DAY
	SQL_C_INTERVAL_HOUR             = SQL_INTERVAL_HOUR
	SQL_C_INTERVAL_MINUTE           = SQL_INTERVAL_MINUTE
	SQL_C_INTERVAL_SECOND           = SQL_INTERVAL_SECOND
	SQL_C_INTERVAL_YEAR_TO_MONTH    = SQL_INTERVAL_YEAR_TO_MONTH
	SQL_C_INTERVAL_DAY_TO_HOUR      = SQL_INTERVAL_DAY_TO_HOUR
	SQL_C_INTERVAL_DAY_TO_MINUTE    = SQL_INTERVAL_DAY_TO_MINUTE
	SQL_C_INTERVAL_DAY_TO_SECOND    = SQL_INTERVAL_DAY_TO_SECOND
	SQL_C_INTERVAL_HOUR_TO_MINUTE   = SQL_INTERVAL_HOUR_TO_MINUTE
	SQL_C_INTERVAL_HOUR_TO_SECOND   = SQL_INTERVAL_HOUR_TO_SECOND
	SQL_C_INTERVAL_MINUTE_TO_SECOND = SQL_INTERVAL_MINUTE_TO_SECOND
)

// SQL_YEAR_MONTH_STRUCT for year-month intervals
type SQL_YEAR_MONTH_STRUCT struct {
	Year  SQLUINTEGER
	Month SQLUINTEGER
}

// SQL_DAY_SECOND_STRUCT for day-time intervals
type SQL_DAY_SECOND_STRUCT struct {
	Day      SQLUINTEGER
	Hour     SQLUINTEGER
	Minute   SQLUINTEGER
	Second   SQLUINTEGER
	Fraction SQLUINTEGER // billionths of a second
}

// SQL_INTERVAL_STRUCT is the ODBC interval structure
type SQL_INTERVAL_STRUCT struct {
	IntervalType SQLSMALLINT
	IntervalSign SQLSMALLINT // 0 = positive, 1 = negative
	_            [4]byte     // padding for alignment
	YearMonth    SQL_YEAR_MONTH_STRUCT
	DaySecond    SQL_DAY_SECOND_STRUCT
}

// IntervalYearMonth represents a year-month interval
type IntervalYearMonth struct {
	Years    int
	Months   int
	Negative bool
}

// IntervalDaySecond represents a day-time interval
type IntervalDaySecond struct {
	Days        int
	Hours       int
	Minutes     int
	Seconds     int
	Nanoseconds int
	Negative    bool
}

// ToDuration converts IntervalDaySecond to time.Duration
func (i IntervalDaySecond) ToDuration() time.Duration {
	d := time.Duration(i.Days)*24*time.Hour +
		time.Duration(i.Hours)*time.Hour +
		time.Duration(i.Minutes)*time.Minute +
		time.Duration(i.Seconds)*time.Second +
		time.Duration(i.Nanoseconds)*time.Nanosecond
	if i.Negative {
		d = -d
	}
	return d
}

// =============================================================================
// Output Parameter Support
// =============================================================================

// ParamDirection specifies the direction of a parameter (input, output, or both)
type ParamDirection int

const (
	// ParamInput is for input-only parameters (default)
	ParamInput ParamDirection = iota
	// ParamOutput is for output-only parameters
	ParamOutput
	// ParamInputOutput is for bidirectional parameters
	ParamInputOutput
)

// OutputParam wraps a value for output or input/output parameter binding.
// Use this type when calling stored procedures that return values through parameters.
type OutputParam struct {
	// Value holds the initial value (for InputOutput) or a type hint (for Output).
	// For output-only parameters, the type of Value determines the buffer size and type.
	// Supported types: int, int32, int64, float32, float64, string, []byte, bool, time.Time
	Value interface{}

	// Direction specifies whether this is an output or input/output parameter
	Direction ParamDirection

	// Size specifies the buffer size for variable-length types (string, []byte).
	// If 0, a default size will be used (4000 for strings, 8000 for bytes).
	Size int
}

// NewOutputParam creates an output-only parameter with the given type hint.
// The type of value determines the expected output type.
func NewOutputParam(typeHint interface{}) OutputParam {
	return OutputParam{
		Value:     typeHint,
		Direction: ParamOutput,
	}
}

// NewOutputParamWithSize creates an output-only parameter with a specific buffer size.
// Use this for variable-length types (string, []byte) when you know the maximum size.
func NewOutputParamWithSize(typeHint interface{}, size int) OutputParam {
	return OutputParam{
		Value:     typeHint,
		Direction: ParamOutput,
		Size:      size,
	}
}

// NewInputOutputParam creates a bidirectional parameter with an initial value.
func NewInputOutputParam(value interface{}) OutputParam {
	return OutputParam{
		Value:     value,
		Direction: ParamInputOutput,
	}
}

// NewInputOutputParamWithSize creates a bidirectional parameter with a specific buffer size.
func NewInputOutputParamWithSize(value interface{}, size int) OutputParam {
	return OutputParam{
		Value:     value,
		Direction: ParamInputOutput,
		Size:      size,
	}
}

// =============================================================================
// Batch Operations Support
// =============================================================================

// Statement attributes for batch operations
const (
	SQL_ATTR_PARAM_BIND_TYPE      SQLINTEGER = 18
	SQL_ATTR_PARAM_STATUS_PTR     SQLINTEGER = 20
	SQL_ATTR_PARAMS_PROCESSED_PTR SQLINTEGER = 21
	SQL_ATTR_PARAMSET_SIZE        SQLINTEGER = 22
)

// Param binding types
const (
	SQL_PARAM_BIND_BY_COLUMN = 0
)

// Param status values
const (
	SQL_PARAM_SUCCESS           = 0
	SQL_PARAM_SUCCESS_WITH_INFO = 1
	SQL_PARAM_ERROR             = 5
	SQL_PARAM_UNUSED            = 7
	SQL_PARAM_DIAG_UNAVAILABLE  = 8
)

// BatchResult holds the result of a batch execution
type BatchResult struct {
	// TotalRowsAffected is the sum of all rows affected across all parameter sets
	TotalRowsAffected int64

	// RowCounts contains the number of rows affected for each parameter set
	RowCounts []int64

	// Errors contains any error that occurred for each parameter set (nil if success)
	Errors []error
}

// HasErrors returns true if any parameter set resulted in an error
func (r *BatchResult) HasErrors() bool {
	for _, err := range r.Errors {
		if err != nil {
			return true
		}
	}
	return false
}

// =============================================================================
// Scrollable Cursor Support
// =============================================================================

// CursorType specifies the type of cursor to use for a query
type CursorType int

const (
	// CursorForwardOnly is the default cursor type (forward-only, read-only)
	CursorForwardOnly CursorType = iota
	// CursorStatic creates a static snapshot of the result set
	CursorStatic
	// CursorKeyset uses a keyset-driven cursor
	CursorKeyset
	// CursorDynamic creates a fully dynamic cursor
	CursorDynamic
)

// Cursor scrollability
const (
	SQL_NONSCROLLABLE = 0
	SQL_SCROLLABLE    = 1
)

// =============================================================================
// LastInsertId Support
// =============================================================================

// LastInsertIdBehavior specifies how LastInsertId() should behave
type LastInsertIdBehavior int

const (
	// LastInsertIdAuto automatically detects the database type and executes
	// the appropriate identity query after INSERT statements
	LastInsertIdAuto LastInsertIdBehavior = iota

	// LastInsertIdDisabled returns 0 for LastInsertId() (original behavior)
	LastInsertIdDisabled

	// LastInsertIdReturning expects the query to use a RETURNING clause (PostgreSQL style)
	LastInsertIdReturning
)
