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
		{"550e8400-e29b-41d4-a716-44665544000g"}, // invalid hex
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
