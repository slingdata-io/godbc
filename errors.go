package odbc

import (
	"fmt"
	"strings"
)

// Error represents an ODBC error with diagnostic information
type Error struct {
	SQLState    string
	NativeError int32
	Message     string
}

// Error implements the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("[%s] %s (native error: %d)", e.SQLState, e.Message, e.NativeError)
}

// DiagRecord represents a single diagnostic record from ODBC
type DiagRecord struct {
	SQLState    string
	NativeError int32
	Message     string
}

// Errors represents multiple ODBC errors
type Errors []Error

// Error implements the error interface for multiple errors
func (e Errors) Error() string {
	if len(e) == 0 {
		return "unknown ODBC error"
	}
	if len(e) == 1 {
		return e[0].Error()
	}
	var sb strings.Builder
	for i, err := range e {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(err.Error())
	}
	return sb.String()
}

// GetDiagRecords retrieves all diagnostic records for a handle
func GetDiagRecords(handleType SQLSMALLINT, handle SQLHANDLE) []DiagRecord {
	var records []DiagRecord
	sqlState := make([]byte, 6)
	message := make([]byte, 1024)

	for i := SQLSMALLINT(1); ; i++ {
		nativeError, msgLen, ret := GetDiagRec(handleType, handle, i, sqlState, message)
		if ret == SQL_NO_DATA {
			break
		}
		if IsSuccess(ret) {
			// Trim null terminator if present
			state := string(sqlState[:5])
			msg := string(message[:msgLen])
			records = append(records, DiagRecord{
				SQLState:    state,
				NativeError: int32(nativeError),
				Message:     msg,
			})
		} else {
			break
		}
	}
	return records
}

// NewError creates an Error from diagnostic records
func NewError(handleType SQLSMALLINT, handle SQLHANDLE) error {
	records := GetDiagRecords(handleType, handle)
	if len(records) == 0 {
		return &Error{
			SQLState: "HY000",
			Message:  "unknown ODBC error",
		}
	}
	if len(records) == 1 {
		return &Error{
			SQLState:    records[0].SQLState,
			NativeError: records[0].NativeError,
			Message:     records[0].Message,
		}
	}
	errors := make(Errors, len(records))
	for i, rec := range records {
		errors[i] = Error{
			SQLState:    rec.SQLState,
			NativeError: rec.NativeError,
			Message:     rec.Message,
		}
	}
	return errors
}

// SQLState constants for common errors
const (
	SQLStateConnectionFailure     = "08001"
	SQLStateConnectionNotOpen     = "08003"
	SQLStateConnectionRejected    = "08004"
	SQLStateConnectionError       = "08S01"
	SQLStateSyntaxError           = "42000"
	SQLStateTableNotFound         = "42S02"
	SQLStateColumnNotFound        = "42S22"
	SQLStateDuplicateKey          = "23000"
	SQLStateConstraintViolation   = "23000"
	SQLStateDataTruncation        = "01004"
	SQLStateInvalidCursorState    = "24000"
	SQLStateInvalidTransState     = "25000"
	SQLStateGeneralError          = "HY000"
	SQLStateMemoryAllocationError = "HY001"
	SQLStateFunctionSequenceError = "HY010"
	SQLStateInvalidStringLength   = "HY090"
	SQLStateInvalidDescIndex      = "HY091"
	SQLStateInvalidAttrValue      = "HY024"
	SQLStateOptionChanged         = "01S02"
)

// IsConnectionError returns true if the error indicates a connection problem
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok {
		switch e.SQLState[:2] {
		case "08": // Connection errors
			return true
		}
	}
	if es, ok := err.(Errors); ok && len(es) > 0 {
		switch es[0].SQLState[:2] {
		case "08":
			return true
		}
	}
	return false
}

// IsDataTruncation returns true if the error indicates data truncation
func IsDataTruncation(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok {
		return e.SQLState == SQLStateDataTruncation
	}
	return false
}

// FormatReturnCode returns a string representation of an ODBC return code
func FormatReturnCode(ret SQLRETURN) string {
	switch ret {
	case SQL_SUCCESS:
		return "SQL_SUCCESS"
	case SQL_SUCCESS_WITH_INFO:
		return "SQL_SUCCESS_WITH_INFO"
	case SQL_ERROR:
		return "SQL_ERROR"
	case SQL_INVALID_HANDLE:
		return "SQL_INVALID_HANDLE"
	case SQL_NO_DATA:
		return "SQL_NO_DATA"
	case SQL_NEED_DATA:
		return "SQL_NEED_DATA"
	case SQL_STILL_EXECUTING:
		return "SQL_STILL_EXECUTING"
	default:
		return fmt.Sprintf("SQLRETURN(%d)", ret)
	}
}
