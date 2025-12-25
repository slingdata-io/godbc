package odbc

import (
	"testing"
	"time"
)

// =============================================================================
// Type Conversion Benchmarks
// =============================================================================

func BenchmarkConvertToODBC_String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convertToODBC("hello world")
	}
}

func BenchmarkConvertToODBC_Int64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convertToODBC(int64(12345))
	}
}

func BenchmarkConvertToODBC_Float64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convertToODBC(float64(3.14159265359))
	}
}

func BenchmarkConvertToODBC_Bool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convertToODBC(true)
	}
}

func BenchmarkConvertToODBC_Bytes(b *testing.B) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		convertToODBC(data)
	}
}

func BenchmarkConvertToODBC_Time(b *testing.B) {
	t := time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		convertToODBC(t)
	}
}

func BenchmarkConvertToODBC_GUID(b *testing.B) {
	guid, _ := ParseGUID("550e8400-e29b-41d4-a716-446655440000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		convertToODBC(guid)
	}
}

func BenchmarkConvertToODBC_Nil(b *testing.B) {
	for i := 0; i < b.N; i++ {
		convertToODBC(nil)
	}
}

// =============================================================================
// UTF-16 Conversion Benchmarks
// =============================================================================

func BenchmarkUTF16ToString_ASCII(b *testing.B) {
	input := []uint16{'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utf16ToString(input)
	}
}

func BenchmarkUTF16ToString_Unicode(b *testing.B) {
	// Chinese characters and ASCII mixed
	input := []uint16{'H', 'i', ' ', 0x4E2D, 0x6587, ' ', 't', 'e', 's', 't'}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utf16ToString(input)
	}
}

func BenchmarkUTF16ToString_SurrogatePairs(b *testing.B) {
	// String with emoji requiring surrogate pairs
	input := []uint16{0xD83D, 0xDE00, ' ', 0xD83D, 0xDE01, ' ', 0xD83D, 0xDE02}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		utf16ToString(input)
	}
}

// =============================================================================
// GUID Parsing Benchmarks
// =============================================================================

func BenchmarkParseGUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseGUID("550e8400-e29b-41d4-a716-446655440000")
	}
}

func BenchmarkSQLGUIDStruct_String(b *testing.B) {
	guid := SQL_GUID_STRUCT{
		Data1: 0x550E8400,
		Data2: 0xE29B,
		Data3: 0x41D4,
		Data4: [8]byte{0xA7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = guid.String()
	}
}

// =============================================================================
// Buffer Pointer Benchmarks
// =============================================================================

func BenchmarkGetBufferPtr_Bytes(b *testing.B) {
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getBufferPtr(buf)
	}
}

func BenchmarkGetBufferPtr_Int64(b *testing.B) {
	val := int64(42)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getBufferPtr(&val)
	}
}

// =============================================================================
// Error Handling Benchmarks
// =============================================================================

func BenchmarkIsConnectionError(b *testing.B) {
	err := &Error{SQLState: "08001", NativeError: 0, Message: "Connection failed"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsConnectionError(err)
	}
}

func BenchmarkIsRetryable(b *testing.B) {
	err := &Error{SQLState: "40001", NativeError: 0, Message: "Deadlock"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsRetryable(err)
	}
}

func BenchmarkError_Error(b *testing.B) {
	err := &Error{SQLState: "42S02", NativeError: 208, Message: "Invalid object name 'foo'"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = err.Error()
	}
}
