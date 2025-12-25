package odbc

import (
	"fmt"
	"strings"
)

// Error represents an ODBC error with diagnostic information from the driver.
// It implements the error interface and provides SQLState, native error code,
// and a human-readable message.
type Error struct {
	SQLState    string
	NativeError int32
	Message     string
}

// Error implements the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("[%s] %s (native error: %d)", e.SQLState, e.Message, e.NativeError)
}

// Unwrap returns nil as Error is a terminal error type.
// This method supports Go 1.13+ error handling with errors.Is and errors.As.
func (e *Error) Unwrap() error {
	return nil
}

// Is reports whether target matches this error's SQLState.
// This allows using errors.Is to check for specific ODBC errors.
func (e *Error) Is(target error) bool {
	if t, ok := target.(*Error); ok {
		return e.SQLState == t.SQLState
	}
	return false
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

// SQLState constants for common errors.
// These follow the ODBC specification and can be used with errors.Is.
const (
	// Connection errors (08xxx)
	SQLStateConnectionFailure     = "08001" // Unable to connect
	SQLStateConnectionNotOpen     = "08003" // Connection not open
	SQLStateConnectionRejected    = "08004" // Connection rejected by server
	SQLStateConnectionError       = "08S01" // Communication link failure

	// Warning states (01xxx)
	SQLStateDataTruncation = "01004" // Data truncated
	SQLStateOptionChanged  = "01S02" // Option value changed

	// No data (02xxx)
	SQLStateNoData = "02000" // No data found

	// Data errors (22xxx)
	SQLStateStringTruncation  = "22001" // String data right truncation
	SQLStateNumericOverflow   = "22003" // Numeric value out of range
	SQLStateInvalidDatetime   = "22007" // Invalid datetime format
	SQLStateDivisionByZero    = "22012" // Division by zero

	// Constraint violations (23xxx)
	SQLStateDuplicateKey        = "23000" // Integrity constraint violation
	SQLStateConstraintViolation = "23000" // Integrity constraint violation (alias)

	// Cursor/Transaction states (24xxx, 25xxx)
	SQLStateInvalidCursorState = "24000" // Invalid cursor state
	SQLStateInvalidTransState  = "25000" // Invalid transaction state

	// Transaction errors (40xxx)
	SQLStateDeadlock          = "40001" // Serialization failure (deadlock)
	SQLStateTransactionFailed = "40003" // Statement completion unknown

	// Syntax/access errors (42xxx)
	SQLStateSyntaxError    = "42000" // Syntax error or access violation
	SQLStateTableNotFound  = "42S02" // Table not found
	SQLStateColumnNotFound = "42S22" // Column not found

	// General errors (HYxxx)
	SQLStateGeneralError          = "HY000" // General error
	SQLStateMemoryAllocationError = "HY001" // Memory allocation error
	SQLStateFunctionSequenceError = "HY010" // Function sequence error
	SQLStateInvalidAttrValue      = "HY024" // Invalid attribute value
	SQLStateInvalidStringLength   = "HY090" // Invalid string or buffer length
	SQLStateInvalidDescIndex      = "HY091" // Invalid descriptor field identifier
	SQLStateTimeout               = "HYT00" // Timeout expired
	SQLStateConnectionTimeout     = "HYT01" // Connection timeout expired
)

// IsConnectionError reports whether err indicates a connection problem.
// Connection errors have SQLState codes starting with "08".
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok {
		if len(e.SQLState) >= 2 && e.SQLState[:2] == "08" {
			return true
		}
	}
	if es, ok := err.(Errors); ok && len(es) > 0 {
		if len(es[0].SQLState) >= 2 && es[0].SQLState[:2] == "08" {
			return true
		}
	}
	return false
}

// IsDataTruncation reports whether err indicates data truncation.
func IsDataTruncation(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok {
		return e.SQLState == SQLStateDataTruncation
	}
	return false
}

// IsRetryable reports whether err represents a transient error that may
// succeed if retried. Transient errors include connection failures,
// timeouts, and deadlocks.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	sqlState := ""
	if e, ok := err.(*Error); ok {
		sqlState = e.SQLState
	} else if es, ok := err.(Errors); ok && len(es) > 0 {
		sqlState = es[0].SQLState
	}
	if sqlState == "" {
		return false
	}

	// Check for retryable SQLStates
	switch sqlState {
	case SQLStateConnectionFailure, SQLStateConnectionError,
		SQLStateDeadlock, SQLStateTimeout, SQLStateConnectionTimeout,
		SQLStateTransactionFailed:
		return true
	}
	// Connection errors (08xxx) are generally retryable
	if len(sqlState) >= 2 && sqlState[:2] == "08" {
		return true
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
