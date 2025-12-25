package odbc

import (
	"reflect"
	"testing"
	"time"
)

// =============================================================================
// Type Conversion Tests (convert.go)
// =============================================================================

func TestConvertToODBC_Nil(t *testing.T) {
	buf, cType, sqlType, _, _, indicator, err := convertToODBC(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf != nil {
		t.Errorf("expected nil buffer, got %v", buf)
	}
	if cType != SQL_C_CHAR {
		t.Errorf("expected SQL_C_CHAR, got %d", cType)
	}
	if sqlType != SQL_VARCHAR {
		t.Errorf("expected SQL_VARCHAR, got %d", sqlType)
	}
	if indicator != SQLLEN(SQL_NULL_DATA) {
		t.Errorf("expected SQL_NULL_DATA indicator, got %d", indicator)
	}
}

func TestConvertToODBC_Bool(t *testing.T) {
	tests := []struct {
		input    bool
		expected byte
	}{
		{true, 1},
		{false, 0},
	}

	for _, tt := range tests {
		buf, cType, sqlType, _, _, _, err := convertToODBC(tt.input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		b, ok := buf.(*byte)
		if !ok {
			t.Fatalf("expected *byte, got %T", buf)
		}
		if *b != tt.expected {
			t.Errorf("input %v: expected %d, got %d", tt.input, tt.expected, *b)
		}
		if cType != SQL_C_BIT {
			t.Errorf("expected SQL_C_BIT, got %d", cType)
		}
		if sqlType != SQL_BIT {
			t.Errorf("expected SQL_BIT, got %d", sqlType)
		}
	}
}

func TestConvertToODBC_Integers(t *testing.T) {
	tests := []struct {
		input   interface{}
		cType   SQLSMALLINT
		sqlType SQLSMALLINT
	}{
		{int8(42), SQL_C_STINYINT, SQL_TINYINT},
		{int16(1000), SQL_C_SSHORT, SQL_SMALLINT},
		{int32(100000), SQL_C_SLONG, SQL_INTEGER},
		{int64(10000000000), SQL_C_SBIGINT, SQL_BIGINT},
		{int(999), SQL_C_SBIGINT, SQL_BIGINT},
	}

	for _, tt := range tests {
		_, cType, sqlType, _, _, _, err := convertToODBC(tt.input)
		if err != nil {
			t.Fatalf("unexpected error for %T: %v", tt.input, err)
		}
		if cType != tt.cType {
			t.Errorf("input %T: expected cType %d, got %d", tt.input, tt.cType, cType)
		}
		if sqlType != tt.sqlType {
			t.Errorf("input %T: expected sqlType %d, got %d", tt.input, tt.sqlType, sqlType)
		}
	}
}

func TestConvertToODBC_UnsignedIntegers(t *testing.T) {
	tests := []struct {
		input   interface{}
		cType   SQLSMALLINT
		sqlType SQLSMALLINT
	}{
		{uint8(42), SQL_C_UTINYINT, SQL_TINYINT},
		{uint16(1000), SQL_C_USHORT, SQL_SMALLINT},
		{uint32(100000), SQL_C_ULONG, SQL_INTEGER},
	}

	for _, tt := range tests {
		_, cType, sqlType, _, _, _, err := convertToODBC(tt.input)
		if err != nil {
			t.Fatalf("unexpected error for %T: %v", tt.input, err)
		}
		if cType != tt.cType {
			t.Errorf("input %T: expected cType %d, got %d", tt.input, tt.cType, cType)
		}
		if sqlType != tt.sqlType {
			t.Errorf("input %T: expected sqlType %d, got %d", tt.input, tt.sqlType, sqlType)
		}
	}
}

func TestConvertToODBC_Uint64(t *testing.T) {
	// uint64 should be converted to string to avoid overflow
	val := uint64(18446744073709551615) // max uint64
	buf, cType, sqlType, _, _, _, err := convertToODBC(val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, ok := buf.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", buf)
	}
	// Should be null-terminated string
	expected := "18446744073709551615"
	if string(b[:len(b)-1]) != expected {
		t.Errorf("expected %q, got %q", expected, string(b[:len(b)-1]))
	}
	if cType != SQL_C_CHAR {
		t.Errorf("expected SQL_C_CHAR, got %d", cType)
	}
	if sqlType != SQL_VARCHAR {
		t.Errorf("expected SQL_VARCHAR, got %d", sqlType)
	}
}

func TestConvertToODBC_Floats(t *testing.T) {
	tests := []struct {
		input   interface{}
		cType   SQLSMALLINT
		sqlType SQLSMALLINT
	}{
		{float32(3.14), SQL_C_FLOAT, SQL_REAL},
		{float64(3.14159265359), SQL_C_DOUBLE, SQL_DOUBLE},
	}

	for _, tt := range tests {
		_, cType, sqlType, _, _, _, err := convertToODBC(tt.input)
		if err != nil {
			t.Fatalf("unexpected error for %T: %v", tt.input, err)
		}
		if cType != tt.cType {
			t.Errorf("input %T: expected cType %d, got %d", tt.input, tt.cType, cType)
		}
		if sqlType != tt.sqlType {
			t.Errorf("input %T: expected sqlType %d, got %d", tt.input, tt.sqlType, sqlType)
		}
	}
}

func TestConvertToODBC_String(t *testing.T) {
	input := "hello world"
	buf, cType, sqlType, colSize, _, indicator, err := convertToODBC(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, ok := buf.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", buf)
	}
	// Should be null-terminated
	if string(b) != input+"\x00" {
		t.Errorf("expected %q, got %q", input+"\x00", string(b))
	}
	if cType != SQL_C_CHAR {
		t.Errorf("expected SQL_C_CHAR, got %d", cType)
	}
	if sqlType != SQL_VARCHAR {
		t.Errorf("expected SQL_VARCHAR, got %d", sqlType)
	}
	if colSize != SQLULEN(len(input)) {
		t.Errorf("expected colSize %d, got %d", len(input), colSize)
	}
	if indicator != SQLLEN(len(input)) {
		t.Errorf("expected indicator %d, got %d", len(input), indicator)
	}
}

func TestConvertToODBC_Bytes(t *testing.T) {
	input := []byte{0x01, 0x02, 0x03, 0x04}
	buf, cType, sqlType, colSize, _, indicator, err := convertToODBC(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, ok := buf.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", buf)
	}
	if !reflect.DeepEqual(b, input) {
		t.Errorf("expected %v, got %v", input, b)
	}
	if cType != SQL_C_BINARY {
		t.Errorf("expected SQL_C_BINARY, got %d", cType)
	}
	if sqlType != SQL_VARBINARY {
		t.Errorf("expected SQL_VARBINARY, got %d", sqlType)
	}
	if colSize != SQLULEN(len(input)) {
		t.Errorf("expected colSize %d, got %d", len(input), colSize)
	}
	if indicator != SQLLEN(len(input)) {
		t.Errorf("expected indicator %d, got %d", len(input), indicator)
	}
}

func TestConvertToODBC_EmptyBytes(t *testing.T) {
	input := []byte{}
	buf, cType, sqlType, colSize, _, indicator, err := convertToODBC(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf != nil {
		t.Errorf("expected nil buffer for empty bytes, got %v", buf)
	}
	if cType != SQL_C_BINARY {
		t.Errorf("expected SQL_C_BINARY, got %d", cType)
	}
	if sqlType != SQL_VARBINARY {
		t.Errorf("expected SQL_VARBINARY, got %d", sqlType)
	}
	if colSize != 0 {
		t.Errorf("expected colSize 0, got %d", colSize)
	}
	if indicator != 0 {
		t.Errorf("expected indicator 0, got %d", indicator)
	}
}

func TestConvertToODBC_Time(t *testing.T) {
	input := time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC)
	buf, cType, sqlType, colSize, decDigits, _, err := convertToODBC(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ts, ok := buf.(*SQL_TIMESTAMP_STRUCT)
	if !ok {
		t.Fatalf("expected *SQL_TIMESTAMP_STRUCT, got %T", buf)
	}
	if ts.Year != 2024 {
		t.Errorf("expected year 2024, got %d", ts.Year)
	}
	if ts.Month != 6 {
		t.Errorf("expected month 6, got %d", ts.Month)
	}
	if ts.Day != 15 {
		t.Errorf("expected day 15, got %d", ts.Day)
	}
	if ts.Hour != 14 {
		t.Errorf("expected hour 14, got %d", ts.Hour)
	}
	if ts.Minute != 30 {
		t.Errorf("expected minute 30, got %d", ts.Minute)
	}
	if ts.Second != 45 {
		t.Errorf("expected second 45, got %d", ts.Second)
	}
	// Fraction should be truncated to milliseconds (123000000)
	expectedFraction := SQLUINTEGER(123000000)
	if ts.Fraction != expectedFraction {
		t.Errorf("expected fraction %d, got %d", expectedFraction, ts.Fraction)
	}
	if cType != SQL_C_TIMESTAMP {
		t.Errorf("expected SQL_C_TIMESTAMP, got %d", cType)
	}
	if sqlType != SQL_TYPE_TIMESTAMP {
		t.Errorf("expected SQL_TYPE_TIMESTAMP, got %d", sqlType)
	}
	if colSize != 23 {
		t.Errorf("expected colSize 23, got %d", colSize)
	}
	if decDigits != 3 {
		t.Errorf("expected decDigits 3, got %d", decDigits)
	}
}

func TestConvertToODBC_GUID(t *testing.T) {
	guid, err := ParseGUID("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("failed to parse GUID: %v", err)
	}
	buf, cType, sqlType, colSize, _, indicator, err := convertToODBC(guid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, ok := buf.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", buf)
	}
	if len(b) != 16 {
		t.Errorf("expected 16 bytes, got %d", len(b))
	}
	if cType != SQL_C_GUID {
		t.Errorf("expected SQL_C_GUID, got %d", cType)
	}
	if sqlType != SQL_GUID {
		t.Errorf("expected SQL_GUID, got %d", sqlType)
	}
	if colSize != 16 {
		t.Errorf("expected colSize 16, got %d", colSize)
	}
	if indicator != 16 {
		t.Errorf("expected indicator 16, got %d", indicator)
	}
}

// =============================================================================
// GUID Tests (convert.go)
// =============================================================================

func TestParseGUID_Valid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"550e8400-e29b-41d4-a716-446655440000"},
		{"00000000-0000-0000-0000-000000000000"},
		{"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}

	for _, tt := range tests {
		_, err := ParseGUID(tt.input)
		if err != nil {
			t.Errorf("ParseGUID(%q) failed: %v", tt.input, err)
		}
	}
}

func TestParseGUID_Invalid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{""},
		{"not-a-guid"},
		{"550e8400-e29b-41d4-a716-44665544000"}, // too short
		{"550e8400-e29b-41d4-a716-4466554400000"}, // too long
		{"550e8400-e29b-41d4-a716-44665544000g"},  // invalid hex
	}

	for _, tt := range tests {
		_, err := ParseGUID(tt.input)
		if err == nil {
			t.Errorf("ParseGUID(%q) should have failed", tt.input)
		}
	}
}

// =============================================================================
// UTF-16 Conversion Tests (rows.go)
// =============================================================================

func TestUTF16ToString_ASCII(t *testing.T) {
	input := []uint16{'H', 'e', 'l', 'l', 'o'}
	expected := "Hello"
	result := utf16ToString(input)
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestUTF16ToString_Unicode(t *testing.T) {
	// Unicode characters that fit in a single UTF-16 code unit
	input := []uint16{0x4E2D, 0x6587} // 中文
	expected := "中文"
	result := utf16ToString(input)
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestUTF16ToString_SurrogatePairs(t *testing.T) {
	// Emoji that requires surrogate pairs: 😀 (U+1F600)
	// High surrogate: 0xD83D, Low surrogate: 0xDE00
	input := []uint16{0xD83D, 0xDE00}
	expected := "😀"
	result := utf16ToString(input)
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestUTF16ToString_Mixed(t *testing.T) {
	// Mix of ASCII, BMP, and surrogate pairs
	// "Hi 中 😀"
	input := []uint16{'H', 'i', ' ', 0x4E2D, ' ', 0xD83D, 0xDE00}
	expected := "Hi 中 😀"
	result := utf16ToString(input)
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestUTF16ToString_Empty(t *testing.T) {
	input := []uint16{}
	expected := ""
	result := utf16ToString(input)
	if result != expected {
		t.Errorf("expected empty string, got %q", result)
	}
}

// =============================================================================
// SQL_GUID_STRUCT Tests (types.go)
// =============================================================================

func TestSQLGUIDStruct_String(t *testing.T) {
	guid := SQL_GUID_STRUCT{
		Data1: 0x550E8400,
		Data2: 0xE29B,
		Data3: 0x41D4,
		Data4: [8]byte{0xA7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
	}
	expected := "550E8400-E29B-41D4-A716-446655440000"
	result := guid.String()
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestSQLGUIDStruct_ZeroGUID(t *testing.T) {
	guid := SQL_GUID_STRUCT{}
	expected := "00000000-0000-0000-0000-000000000000"
	result := guid.String()
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// =============================================================================
// SQL Type Name Tests (convert.go)
// =============================================================================

func TestSQLTypeName(t *testing.T) {
	tests := []struct {
		sqlType  SQLSMALLINT
		expected string
	}{
		{SQL_CHAR, "CHAR"},
		{SQL_VARCHAR, "VARCHAR"},
		{SQL_LONGVARCHAR, "LONGVARCHAR"},
		{SQL_WCHAR, "WCHAR"},
		{SQL_WVARCHAR, "WVARCHAR"},
		{SQL_WLONGVARCHAR, "WLONGVARCHAR"},
		{SQL_DECIMAL, "DECIMAL"},
		{SQL_NUMERIC, "NUMERIC"},
		{SQL_SMALLINT, "SMALLINT"},
		{SQL_INTEGER, "INTEGER"},
		{SQL_REAL, "REAL"},
		{SQL_FLOAT, "FLOAT"},
		{SQL_DOUBLE, "DOUBLE"},
		{SQL_BIT, "BIT"},
		{SQL_TINYINT, "TINYINT"},
		{SQL_BIGINT, "BIGINT"},
		{SQL_BINARY, "BINARY"},
		{SQL_VARBINARY, "VARBINARY"},
		{SQL_LONGVARBINARY, "LONGVARBINARY"},
		{SQL_TYPE_DATE, "DATE"},
		{SQL_TYPE_TIME, "TIME"},
		{SQL_TYPE_TIMESTAMP, "TIMESTAMP"},
		{SQL_DATETIME, "DATETIME"},
		{SQL_GUID, "GUID"},
	}

	for _, tt := range tests {
		result := SQLTypeName(tt.sqlType)
		if result != tt.expected {
			t.Errorf("SQLTypeName(%d): expected %q, got %q", tt.sqlType, tt.expected, result)
		}
	}
}

func TestSQLTypeName_Unknown(t *testing.T) {
	result := SQLTypeName(9999)
	if result != "UNKNOWN(9999)" {
		t.Errorf("expected UNKNOWN(9999), got %q", result)
	}
}

// =============================================================================
// Error Tests (errors.go)
// =============================================================================

func TestError_Error(t *testing.T) {
	err := &Error{
		SQLState:    "42S02",
		NativeError: 208,
		Message:     "Invalid object name 'foo'",
	}
	result := err.Error()
	expected := "[42S02] Invalid object name 'foo' (native error: 208)"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestErrors_Error(t *testing.T) {
	errs := Errors{
		{SQLState: "42S02", NativeError: 208, Message: "Error 1"},
		{SQLState: "42000", NativeError: 156, Message: "Error 2"},
	}
	result := errs.Error()
	if result == "" {
		t.Error("expected non-empty error string")
	}
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{&Error{SQLState: "08001"}, true},
		{&Error{SQLState: "08003"}, true},
		{&Error{SQLState: "08004"}, true},
		{&Error{SQLState: "08S01"}, true},
		{&Error{SQLState: "42S02"}, false},
		{&Error{SQLState: "00000"}, false},
		{Errors{{SQLState: "08001"}}, true},
		{Errors{{SQLState: "42S02"}}, false},
		{nil, false},
	}

	for _, tt := range tests {
		result := IsConnectionError(tt.err)
		if result != tt.expected {
			t.Errorf("IsConnectionError(%v): expected %v, got %v", tt.err, tt.expected, result)
		}
	}
}

func TestIsDataTruncation(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{&Error{SQLState: "01004"}, true},
		{&Error{SQLState: "42S02"}, false},
		{&Error{SQLState: "00000"}, false},
		// Note: IsDataTruncation only checks *Error, not Errors slice
		{nil, false},
	}

	for _, tt := range tests {
		result := IsDataTruncation(tt.err)
		if result != tt.expected {
			t.Errorf("IsDataTruncation(%v): expected %v, got %v", tt.err, tt.expected, result)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		// Connection errors are retryable
		{&Error{SQLState: "08001"}, true},
		{&Error{SQLState: "08S01"}, true},
		// Deadlock is retryable
		{&Error{SQLState: "40001"}, true},
		// Timeout is retryable
		{&Error{SQLState: "HYT00"}, true},
		{&Error{SQLState: "HYT01"}, true},
		// Transaction failed is retryable
		{&Error{SQLState: "40003"}, true},
		// Syntax errors are not retryable
		{&Error{SQLState: "42S02"}, false},
		{&Error{SQLState: "42000"}, false},
		// Constraint violations are not retryable
		{&Error{SQLState: "23000"}, false},
		// Success states are not retryable
		{&Error{SQLState: "00000"}, false},
		// Nil is not retryable
		{nil, false},
		// Errors slice with connection error
		{Errors{{SQLState: "08001"}}, true},
		// Errors slice with non-retryable error
		{Errors{{SQLState: "42S02"}}, false},
	}

	for _, tt := range tests {
		result := IsRetryable(tt.err)
		if result != tt.expected {
			t.Errorf("IsRetryable(%v): expected %v, got %v", tt.err, tt.expected, result)
		}
	}
}

func TestError_Is(t *testing.T) {
	err1 := &Error{SQLState: "42S02", NativeError: 208, Message: "Table not found"}
	err2 := &Error{SQLState: "42S02", NativeError: 100, Message: "Different message"}
	err3 := &Error{SQLState: "08001", NativeError: 0, Message: "Connection error"}

	// Same SQLState should match
	if !err1.Is(err2) {
		t.Error("expected err1.Is(err2) to be true (same SQLState)")
	}

	// Different SQLState should not match
	if err1.Is(err3) {
		t.Error("expected err1.Is(err3) to be false (different SQLState)")
	}

	// Non-Error type should not match
	if err1.Is(nil) {
		t.Error("expected err1.Is(nil) to be false")
	}
}

func TestError_Unwrap(t *testing.T) {
	err := &Error{SQLState: "42S02", NativeError: 208, Message: "Test"}
	if err.Unwrap() != nil {
		t.Error("expected Unwrap to return nil")
	}
}

// =============================================================================
// IsSuccess Tests (types.go)
// =============================================================================

func TestIsSuccess(t *testing.T) {
	tests := []struct {
		ret      SQLRETURN
		expected bool
	}{
		{SQL_SUCCESS, true},
		{SQL_SUCCESS_WITH_INFO, true},
		{SQL_ERROR, false},
		{SQL_INVALID_HANDLE, false},
		{SQL_NO_DATA, false},
		{SQL_NEED_DATA, false},
		{SQL_STILL_EXECUTING, false},
	}

	for _, tt := range tests {
		result := IsSuccess(tt.ret)
		if result != tt.expected {
			t.Errorf("IsSuccess(%d): expected %v, got %v", tt.ret, tt.expected, result)
		}
	}
}

// =============================================================================
// getBufferPtr Tests (convert.go)
// =============================================================================

func TestGetBufferPtr_Bytes(t *testing.T) {
	buf := []byte{1, 2, 3, 4}
	ptr, length := getBufferPtr(buf)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length != 4 {
		t.Errorf("expected length 4, got %d", length)
	}
}

func TestGetBufferPtr_EmptyBytes(t *testing.T) {
	buf := []byte{}
	ptr, length := getBufferPtr(buf)
	if ptr != 0 {
		t.Errorf("expected zero pointer for empty slice, got %d", ptr)
	}
	if length != 0 {
		t.Errorf("expected length 0, got %d", length)
	}
}

func TestGetBufferPtr_Int64(t *testing.T) {
	val := int64(42)
	ptr, length := getBufferPtr(&val)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length != 8 {
		t.Errorf("expected length 8, got %d", length)
	}
}

func TestGetBufferPtr_Float64(t *testing.T) {
	val := float64(3.14)
	ptr, length := getBufferPtr(&val)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length != 8 {
		t.Errorf("expected length 8, got %d", length)
	}
}

func TestGetBufferPtr_Timestamp(t *testing.T) {
	ts := SQL_TIMESTAMP_STRUCT{Year: 2024, Month: 6, Day: 15}
	ptr, length := getBufferPtr(&ts)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length == 0 {
		t.Error("expected non-zero length")
	}
}

func TestGetBufferPtr_Nil(t *testing.T) {
	ptr, length := getBufferPtr(nil)
	if ptr != 0 {
		t.Errorf("expected zero pointer for nil, got %d", ptr)
	}
	if length != 0 {
		t.Errorf("expected length 0 for nil, got %d", length)
	}
}

// =============================================================================
// Enhanced Type Handling Tests
// =============================================================================

// Timestamp Precision Tests

func TestTruncateFraction(t *testing.T) {
	tests := []struct {
		nanos     int
		precision TimestampPrecision
		expected  SQLUINTEGER
	}{
		{123456789, TimestampPrecisionSeconds, 0},
		{123456789, TimestampPrecisionMilliseconds, 123000000},
		{123456789, TimestampPrecisionMicroseconds, 123456000},
		{123456789, TimestampPrecisionNanoseconds, 123456789},
		{0, TimestampPrecisionMilliseconds, 0},
		{999999999, TimestampPrecisionMilliseconds, 999000000},
	}

	for _, tt := range tests {
		result := truncateFraction(tt.nanos, tt.precision)
		if result != tt.expected {
			t.Errorf("truncateFraction(%d, %d): expected %d, got %d", tt.nanos, tt.precision, tt.expected, result)
		}
	}
}

func TestTimestampColumnSize(t *testing.T) {
	tests := []struct {
		precision TimestampPrecision
		expected  SQLULEN
	}{
		{TimestampPrecisionSeconds, 19},
		{TimestampPrecisionMilliseconds, 23},
		{TimestampPrecisionMicroseconds, 26},
		{TimestampPrecisionNanoseconds, 29},
	}

	for _, tt := range tests {
		result := timestampColumnSize(tt.precision)
		if result != tt.expected {
			t.Errorf("timestampColumnSize(%d): expected %d, got %d", tt.precision, tt.expected, result)
		}
	}
}

func TestConvertToODBC_Timestamp(t *testing.T) {
	input := time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC)
	ts := NewTimestamp(input, TimestampPrecisionMicroseconds)

	buf, cType, sqlType, colSize, decDigits, _, err := convertToODBC(ts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tsStruct, ok := buf.(*SQL_TIMESTAMP_STRUCT)
	if !ok {
		t.Fatalf("expected *SQL_TIMESTAMP_STRUCT, got %T", buf)
	}

	// Check fraction is truncated to microseconds
	expectedFraction := SQLUINTEGER(123456000)
	if tsStruct.Fraction != expectedFraction {
		t.Errorf("expected fraction %d, got %d", expectedFraction, tsStruct.Fraction)
	}

	if cType != SQL_C_TIMESTAMP {
		t.Errorf("expected SQL_C_TIMESTAMP, got %d", cType)
	}
	if sqlType != SQL_TYPE_TIMESTAMP {
		t.Errorf("expected SQL_TYPE_TIMESTAMP, got %d", sqlType)
	}
	if colSize != 26 {
		t.Errorf("expected colSize 26, got %d", colSize)
	}
	if decDigits != 6 {
		t.Errorf("expected decDigits 6, got %d", decDigits)
	}
}

// UTF-16 / WideString Tests

func TestStringToUTF16_ASCII(t *testing.T) {
	result := stringToUTF16("Hello")
	expected := []uint16{'H', 'e', 'l', 'l', 'o', 0}
	if len(result) != len(expected) {
		t.Fatalf("expected length %d, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("at index %d: expected %d, got %d", i, expected[i], result[i])
		}
	}
}

func TestStringToUTF16_Unicode(t *testing.T) {
	result := stringToUTF16("中文")
	// 中 = 0x4E2D, 文 = 0x6587, plus null terminator
	expected := []uint16{0x4E2D, 0x6587, 0}
	if len(result) != len(expected) {
		t.Fatalf("expected length %d, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("at index %d: expected 0x%04X, got 0x%04X", i, expected[i], result[i])
		}
	}
}

func TestStringToUTF16_SurrogatePairs(t *testing.T) {
	// Emoji 😀 (U+1F600) requires surrogate pairs: 0xD83D 0xDE00
	result := stringToUTF16("😀")
	expected := []uint16{0xD83D, 0xDE00, 0}
	if len(result) != len(expected) {
		t.Fatalf("expected length %d, got %d", len(expected), len(result))
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("at index %d: expected 0x%04X, got 0x%04X", i, expected[i], result[i])
		}
	}
}

func TestConvertToODBC_WideString(t *testing.T) {
	input := WideString("Hello中文")
	buf, cType, sqlType, colSize, _, indicator, err := convertToODBC(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	utf16Buf, ok := buf.([]uint16)
	if !ok {
		t.Fatalf("expected []uint16, got %T", buf)
	}

	// "Hello中文" = 7 characters + null terminator
	if len(utf16Buf) != 8 {
		t.Errorf("expected buffer length 8, got %d", len(utf16Buf))
	}

	if cType != SQL_C_WCHAR {
		t.Errorf("expected SQL_C_WCHAR, got %d", cType)
	}
	if sqlType != SQL_WVARCHAR {
		t.Errorf("expected SQL_WVARCHAR, got %d", sqlType)
	}
	if colSize != 7 { // Character count
		t.Errorf("expected colSize 7, got %d", colSize)
	}
	if indicator != 14 { // Byte count (7 * 2)
		t.Errorf("expected indicator 14, got %d", indicator)
	}
}

func TestGetBufferPtr_Uint16Slice(t *testing.T) {
	buf := []uint16{0x0048, 0x0069, 0}
	ptr, length := getBufferPtr(buf)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length != 6 { // 3 code units * 2 bytes
		t.Errorf("expected length 6, got %d", length)
	}
}

// Decimal Tests

func TestNewDecimal_Valid(t *testing.T) {
	tests := []struct {
		value     string
		precision int
		scale     int
	}{
		{"123.45", 5, 2},
		{"-999.99", 5, 2},
		{"0", 1, 0},
		{"12345678901234567890123456789012345678", 38, 0},
	}

	for _, tt := range tests {
		d, err := NewDecimal(tt.value, tt.precision, tt.scale)
		if err != nil {
			t.Errorf("NewDecimal(%q, %d, %d) failed: %v", tt.value, tt.precision, tt.scale, err)
			continue
		}
		if d.Value != tt.value {
			t.Errorf("expected value %q, got %q", tt.value, d.Value)
		}
		if d.Precision != tt.precision {
			t.Errorf("expected precision %d, got %d", tt.precision, d.Precision)
		}
		if d.Scale != tt.scale {
			t.Errorf("expected scale %d, got %d", tt.scale, d.Scale)
		}
	}
}

func TestNewDecimal_InvalidPrecision(t *testing.T) {
	_, err := NewDecimal("123", 0, 0)
	if err == nil {
		t.Error("expected error for precision 0")
	}

	_, err = NewDecimal("123", 39, 0)
	if err == nil {
		t.Error("expected error for precision 39")
	}
}

func TestNewDecimal_InvalidScale(t *testing.T) {
	_, err := NewDecimal("123", 5, -1)
	if err == nil {
		t.Error("expected error for negative scale")
	}

	_, err = NewDecimal("123", 5, 6)
	if err == nil {
		t.Error("expected error for scale > precision")
	}
}

func TestParseDecimal(t *testing.T) {
	tests := []struct {
		input     string
		precision int
		scale     int
	}{
		{"123.45", 5, 2},
		{"-999.99", 5, 2},
		{"42", 2, 0},
		{"+100", 3, 0},
		{"0.001", 4, 3},
	}

	for _, tt := range tests {
		d, err := ParseDecimal(tt.input)
		if err != nil {
			t.Errorf("ParseDecimal(%q) failed: %v", tt.input, err)
			continue
		}
		if d.Precision != tt.precision {
			t.Errorf("ParseDecimal(%q): expected precision %d, got %d", tt.input, tt.precision, d.Precision)
		}
		if d.Scale != tt.scale {
			t.Errorf("ParseDecimal(%q): expected scale %d, got %d", tt.input, tt.scale, d.Scale)
		}
	}
}

func TestParseDecimal_Invalid(t *testing.T) {
	invalids := []string{"", "abc", "12.34.56", "--123", "++123"}
	for _, s := range invalids {
		_, err := ParseDecimal(s)
		if err == nil {
			t.Errorf("ParseDecimal(%q) should have failed", s)
		}
	}
}

func TestConvertToODBC_Decimal(t *testing.T) {
	d, _ := NewDecimal("123.45", 10, 2)
	buf, cType, sqlType, colSize, decDigits, indicator, err := convertToODBC(d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, ok := buf.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", buf)
	}
	// Should be null-terminated string
	if string(b[:len(b)-1]) != "123.45" {
		t.Errorf("expected buffer \"123.45\", got %q", string(b[:len(b)-1]))
	}

	if cType != SQL_C_CHAR {
		t.Errorf("expected SQL_C_CHAR, got %d", cType)
	}
	if sqlType != SQL_DECIMAL {
		t.Errorf("expected SQL_DECIMAL, got %d", sqlType)
	}
	if colSize != 10 {
		t.Errorf("expected colSize 10, got %d", colSize)
	}
	if decDigits != 2 {
		t.Errorf("expected decDigits 2, got %d", decDigits)
	}
	if indicator != 6 { // Length of "123.45"
		t.Errorf("expected indicator 6, got %d", indicator)
	}
}

// Interval Tests

func TestIntervalDaySecond_ToDuration(t *testing.T) {
	tests := []struct {
		interval IntervalDaySecond
		expected time.Duration
	}{
		{IntervalDaySecond{Days: 1}, 24 * time.Hour},
		{IntervalDaySecond{Hours: 2, Minutes: 30}, 2*time.Hour + 30*time.Minute},
		{IntervalDaySecond{Seconds: 90}, 90 * time.Second},
		{IntervalDaySecond{Days: 1, Negative: true}, -24 * time.Hour},
		{IntervalDaySecond{Nanoseconds: 1000000}, time.Millisecond},
	}

	for _, tt := range tests {
		result := tt.interval.ToDuration()
		if result != tt.expected {
			t.Errorf("ToDuration() for %+v: expected %v, got %v", tt.interval, tt.expected, result)
		}
	}
}

func TestConvertToODBC_IntervalYearMonth(t *testing.T) {
	i := IntervalYearMonth{Years: 2, Months: 6, Negative: false}
	buf, cType, sqlType, _, _, _, err := convertToODBC(i)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	is, ok := buf.(*SQL_INTERVAL_STRUCT)
	if !ok {
		t.Fatalf("expected *SQL_INTERVAL_STRUCT, got %T", buf)
	}

	if is.IntervalType != SQL_INTERVAL_YEAR_TO_MONTH {
		t.Errorf("expected IntervalType %d, got %d", SQL_INTERVAL_YEAR_TO_MONTH, is.IntervalType)
	}
	if is.IntervalSign != 0 {
		t.Errorf("expected IntervalSign 0, got %d", is.IntervalSign)
	}
	if is.YearMonth.Year != 2 {
		t.Errorf("expected Year 2, got %d", is.YearMonth.Year)
	}
	if is.YearMonth.Month != 6 {
		t.Errorf("expected Month 6, got %d", is.YearMonth.Month)
	}

	if cType != SQL_C_INTERVAL_YEAR_TO_MONTH {
		t.Errorf("expected SQL_C_INTERVAL_YEAR_TO_MONTH, got %d", cType)
	}
	if sqlType != SQL_INTERVAL_YEAR_TO_MONTH {
		t.Errorf("expected SQL_INTERVAL_YEAR_TO_MONTH, got %d", sqlType)
	}
}

func TestConvertToODBC_IntervalDaySecond(t *testing.T) {
	i := IntervalDaySecond{Days: 5, Hours: 12, Minutes: 30, Seconds: 45, Negative: true}
	buf, cType, sqlType, _, _, _, err := convertToODBC(i)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	is, ok := buf.(*SQL_INTERVAL_STRUCT)
	if !ok {
		t.Fatalf("expected *SQL_INTERVAL_STRUCT, got %T", buf)
	}

	if is.IntervalSign != 1 { // Negative
		t.Errorf("expected IntervalSign 1 (negative), got %d", is.IntervalSign)
	}
	if is.DaySecond.Day != 5 {
		t.Errorf("expected Day 5, got %d", is.DaySecond.Day)
	}
	if is.DaySecond.Hour != 12 {
		t.Errorf("expected Hour 12, got %d", is.DaySecond.Hour)
	}
	if is.DaySecond.Minute != 30 {
		t.Errorf("expected Minute 30, got %d", is.DaySecond.Minute)
	}
	if is.DaySecond.Second != 45 {
		t.Errorf("expected Second 45, got %d", is.DaySecond.Second)
	}

	if cType != SQL_C_INTERVAL_DAY_TO_SECOND {
		t.Errorf("expected SQL_C_INTERVAL_DAY_TO_SECOND, got %d", cType)
	}
	if sqlType != SQL_INTERVAL_DAY_TO_SECOND {
		t.Errorf("expected SQL_INTERVAL_DAY_TO_SECOND, got %d", sqlType)
	}
}

func TestGetBufferPtr_IntervalStruct(t *testing.T) {
	is := SQL_INTERVAL_STRUCT{IntervalType: SQL_INTERVAL_DAY}
	ptr, length := getBufferPtr(&is)
	if ptr == 0 {
		t.Error("expected non-zero pointer")
	}
	if length == 0 {
		t.Error("expected non-zero length")
	}
}

// TimestampTZ Tests

func TestConvertToODBC_TimestampTZ(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	input := time.Date(2024, 6, 15, 14, 30, 0, 0, loc)
	ts := NewTimestampTZ(input, TimestampPrecisionMilliseconds, loc)

	buf, cType, sqlType, _, _, _, err := convertToODBC(ts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tsStruct, ok := buf.(*SQL_TIMESTAMP_STRUCT)
	if !ok {
		t.Fatalf("expected *SQL_TIMESTAMP_STRUCT, got %T", buf)
	}

	// Should be converted to UTC: 14:30 EDT = 18:30 UTC
	utcTime := input.UTC()
	if tsStruct.Hour != SQLUSMALLINT(utcTime.Hour()) {
		t.Errorf("expected UTC hour %d, got %d", utcTime.Hour(), tsStruct.Hour)
	}

	if cType != SQL_C_TIMESTAMP {
		t.Errorf("expected SQL_C_TIMESTAMP, got %d", cType)
	}
	if sqlType != SQL_TYPE_TIMESTAMP {
		t.Errorf("expected SQL_TYPE_TIMESTAMP, got %d", sqlType)
	}
}

// SQL Type Name Tests for Interval Types

func TestSQLTypeName_Intervals(t *testing.T) {
	tests := []struct {
		sqlType  SQLSMALLINT
		expected string
	}{
		{SQL_INTERVAL_YEAR, "INTERVAL YEAR"},
		{SQL_INTERVAL_MONTH, "INTERVAL MONTH"},
		{SQL_INTERVAL_DAY, "INTERVAL DAY"},
		{SQL_INTERVAL_HOUR, "INTERVAL HOUR"},
		{SQL_INTERVAL_MINUTE, "INTERVAL MINUTE"},
		{SQL_INTERVAL_SECOND, "INTERVAL SECOND"},
		{SQL_INTERVAL_YEAR_TO_MONTH, "INTERVAL YEAR TO MONTH"},
		{SQL_INTERVAL_DAY_TO_HOUR, "INTERVAL DAY TO HOUR"},
		{SQL_INTERVAL_DAY_TO_MINUTE, "INTERVAL DAY TO MINUTE"},
		{SQL_INTERVAL_DAY_TO_SECOND, "INTERVAL DAY TO SECOND"},
		{SQL_INTERVAL_HOUR_TO_MINUTE, "INTERVAL HOUR TO MINUTE"},
		{SQL_INTERVAL_HOUR_TO_SECOND, "INTERVAL HOUR TO SECOND"},
		{SQL_INTERVAL_MINUTE_TO_SECOND, "INTERVAL MINUTE TO SECOND"},
	}

	for _, tt := range tests {
		result := SQLTypeName(tt.sqlType)
		if result != tt.expected {
			t.Errorf("SQLTypeName(%d): expected %q, got %q", tt.sqlType, tt.expected, result)
		}
	}
}

// isValidDecimalString Tests

func TestIsValidDecimalString(t *testing.T) {
	valid := []string{"123", "-123", "+123", "123.45", "-0.5", "0", ".5", "5."}
	for _, s := range valid {
		if !isValidDecimalString(s) {
			t.Errorf("isValidDecimalString(%q) should return true", s)
		}
	}

	invalid := []string{"", "-", "+", "abc", "12.34.56", "1e10"}
	for _, s := range invalid {
		if isValidDecimalString(s) {
			t.Errorf("isValidDecimalString(%q) should return false", s)
		}
	}
}
