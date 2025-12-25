package odbc

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

var (
	odbcLib  uintptr
	initOnce sync.Once
	initErr  error
)

// ODBC function pointers - populated by purego
var (
	sqlAllocHandle    func(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN
	sqlFreeHandle     func(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN
	sqlSetEnvAttr     func(env SQLHENV, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN
	sqlGetEnvAttr     func(env SQLHENV, attribute SQLINTEGER, value uintptr, bufferLength SQLINTEGER, stringLength *SQLINTEGER) SQLRETURN
	sqlDriverConnect  func(dbc SQLHDBC, hwnd uintptr, inConnStr *byte, inConnStrLen SQLSMALLINT, outConnStr *byte, outConnStrMax SQLSMALLINT, outConnStrLen *SQLSMALLINT, driverCompletion SQLUSMALLINT) SQLRETURN
	sqlDisconnect     func(dbc SQLHDBC) SQLRETURN
	sqlSetConnectAttr func(dbc SQLHDBC, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN
	sqlGetConnectAttr func(dbc SQLHDBC, attribute SQLINTEGER, value uintptr, bufferLength SQLINTEGER, stringLength *SQLINTEGER) SQLRETURN
	sqlGetInfo        func(dbc SQLHDBC, infoType SQLUSMALLINT, infoValue uintptr, bufferLength SQLSMALLINT, stringLength *SQLSMALLINT) SQLRETURN
	sqlExecDirect     func(stmt SQLHSTMT, stmtText *byte, textLength SQLINTEGER) SQLRETURN
	sqlPrepare        func(stmt SQLHSTMT, stmtText *byte, textLength SQLINTEGER) SQLRETURN
	sqlExecute        func(stmt SQLHSTMT) SQLRETURN
	sqlNumResultCols  func(stmt SQLHSTMT, columnCount *SQLSMALLINT) SQLRETURN
	sqlDescribeCol    func(stmt SQLHSTMT, colNum SQLUSMALLINT, colName *byte, bufferLen SQLSMALLINT, nameLen *SQLSMALLINT, dataType *SQLSMALLINT, colSize *SQLULEN, decDigits *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN
	sqlColAttribute   func(stmt SQLHSTMT, colNum SQLUSMALLINT, fieldId SQLUSMALLINT, charAttr uintptr, bufferLen SQLSMALLINT, strLen *SQLSMALLINT, numAttr *SQLLEN) SQLRETURN
	sqlBindCol        func(stmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue uintptr, bufferLen SQLLEN, strLenOrInd *SQLLEN) SQLRETURN
	sqlBindParameter  func(stmt SQLHSTMT, paramNum SQLUSMALLINT, ioType SQLSMALLINT, valueType SQLSMALLINT, paramType SQLSMALLINT, colSize SQLULEN, decDigits SQLSMALLINT, paramValue uintptr, bufferLen SQLLEN, strLenOrInd *SQLLEN) SQLRETURN
	sqlFetch          func(stmt SQLHSTMT) SQLRETURN
	sqlFetchScroll    func(stmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN
	sqlGetData        func(stmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue uintptr, bufferLen SQLLEN, strLenOrInd *SQLLEN) SQLRETURN
	sqlRowCount       func(stmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN
	sqlNumParams      func(stmt SQLHSTMT, paramCount *SQLSMALLINT) SQLRETURN
	sqlDescribeParam  func(stmt SQLHSTMT, paramNum SQLUSMALLINT, dataType *SQLSMALLINT, paramSize *SQLULEN, decDigits *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN
	sqlGetDiagRec     func(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState *byte, nativeError *SQLINTEGER, msgText *byte, bufferLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN
	sqlGetDiagField   func(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, diagId SQLSMALLINT, diagInfo uintptr, bufferLen SQLSMALLINT, stringLen *SQLSMALLINT) SQLRETURN
	sqlEndTran        func(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN
	sqlCloseCursor    func(stmt SQLHSTMT) SQLRETURN
	sqlCancel         func(stmt SQLHSTMT) SQLRETURN
	sqlFreeStmt       func(stmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN
	sqlMoreResults    func(stmt SQLHSTMT) SQLRETURN
	sqlSetStmtAttr    func(stmt SQLHSTMT, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN
	sqlGetStmtAttr    func(stmt SQLHSTMT, attribute SQLINTEGER, value uintptr, bufferLength SQLINTEGER, stringLength *SQLINTEGER) SQLRETURN
	sqlTables         func(stmt SQLHSTMT, catalogName *byte, nameLen1 SQLSMALLINT, schemaName *byte, nameLen2 SQLSMALLINT, tableName *byte, nameLen3 SQLSMALLINT, tableType *byte, nameLen4 SQLSMALLINT) SQLRETURN
	sqlColumns        func(stmt SQLHSTMT, catalogName *byte, nameLen1 SQLSMALLINT, schemaName *byte, nameLen2 SQLSMALLINT, tableName *byte, nameLen3 SQLSMALLINT, columnName *byte, nameLen4 SQLSMALLINT) SQLRETURN
)

// getLibraryPath returns the platform-specific ODBC library path.
// The GODBC_LIBRARY_PATH environment variable can override the default path.
func getLibraryPath() string {
	// Check environment variable first
	if path := os.Getenv("GODBC_LIBRARY_PATH"); path != "" {
		return path
	}

	switch runtime.GOOS {
	case "windows":
		return "odbc32.dll"
	case "darwin":
		// Check common macOS locations for unixODBC
		paths := []string{
			"/opt/homebrew/lib/libodbc.2.dylib", // Apple Silicon Homebrew
			"/usr/local/lib/libodbc.2.dylib",    // Intel Homebrew
			"/opt/homebrew/lib/libodbc.dylib",
			"/usr/local/lib/libodbc.dylib",
		}
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
		return "libodbc.2.dylib" // Let purego search standard paths
	default:
		// Linux and other Unix-like systems
		return "libodbc.so.2"
	}
}

// initODBC initializes the ODBC library and registers all functions.
// If loading fails, set GODBC_LIBRARY_PATH to specify a custom library location.
func initODBC() error {
	initOnce.Do(func() {
		libPath := getLibraryPath()

		// Use platform-specific library loading (implemented in odbc_windows.go and odbc_unix.go)
		odbcLib, initErr = loadODBCLibrary(libPath)
		if initErr != nil {
			initErr = fmt.Errorf("failed to load ODBC library %q: %w (set GODBC_LIBRARY_PATH to override)", libPath, initErr)
			return
		}

		// Register core handle management functions
		purego.RegisterLibFunc(&sqlAllocHandle, odbcLib, "SQLAllocHandle")
		purego.RegisterLibFunc(&sqlFreeHandle, odbcLib, "SQLFreeHandle")

		// Register environment functions
		purego.RegisterLibFunc(&sqlSetEnvAttr, odbcLib, "SQLSetEnvAttr")
		purego.RegisterLibFunc(&sqlGetEnvAttr, odbcLib, "SQLGetEnvAttr")

		// Register connection functions
		// Use ANSI versions on Unix, which don't have 'A' suffix
		if runtime.GOOS == "windows" {
			purego.RegisterLibFunc(&sqlDriverConnect, odbcLib, "SQLDriverConnectA")
			purego.RegisterLibFunc(&sqlGetInfo, odbcLib, "SQLGetInfoA")
		} else {
			purego.RegisterLibFunc(&sqlDriverConnect, odbcLib, "SQLDriverConnect")
			purego.RegisterLibFunc(&sqlGetInfo, odbcLib, "SQLGetInfo")
		}
		purego.RegisterLibFunc(&sqlDisconnect, odbcLib, "SQLDisconnect")
		purego.RegisterLibFunc(&sqlSetConnectAttr, odbcLib, "SQLSetConnectAttr")
		purego.RegisterLibFunc(&sqlGetConnectAttr, odbcLib, "SQLGetConnectAttr")

		// Register statement functions
		if runtime.GOOS == "windows" {
			purego.RegisterLibFunc(&sqlExecDirect, odbcLib, "SQLExecDirectA")
			purego.RegisterLibFunc(&sqlPrepare, odbcLib, "SQLPrepareA")
			purego.RegisterLibFunc(&sqlDescribeCol, odbcLib, "SQLDescribeColA")
			purego.RegisterLibFunc(&sqlColAttribute, odbcLib, "SQLColAttributeA")
			purego.RegisterLibFunc(&sqlGetDiagRec, odbcLib, "SQLGetDiagRecA")
			purego.RegisterLibFunc(&sqlTables, odbcLib, "SQLTablesA")
			purego.RegisterLibFunc(&sqlColumns, odbcLib, "SQLColumnsA")
		} else {
			purego.RegisterLibFunc(&sqlExecDirect, odbcLib, "SQLExecDirect")
			purego.RegisterLibFunc(&sqlPrepare, odbcLib, "SQLPrepare")
			purego.RegisterLibFunc(&sqlDescribeCol, odbcLib, "SQLDescribeCol")
			purego.RegisterLibFunc(&sqlColAttribute, odbcLib, "SQLColAttribute")
			purego.RegisterLibFunc(&sqlGetDiagRec, odbcLib, "SQLGetDiagRec")
			purego.RegisterLibFunc(&sqlTables, odbcLib, "SQLTables")
			purego.RegisterLibFunc(&sqlColumns, odbcLib, "SQLColumns")
		}
		purego.RegisterLibFunc(&sqlExecute, odbcLib, "SQLExecute")
		purego.RegisterLibFunc(&sqlNumResultCols, odbcLib, "SQLNumResultCols")
		purego.RegisterLibFunc(&sqlBindCol, odbcLib, "SQLBindCol")
		purego.RegisterLibFunc(&sqlBindParameter, odbcLib, "SQLBindParameter")
		purego.RegisterLibFunc(&sqlFetch, odbcLib, "SQLFetch")
		purego.RegisterLibFunc(&sqlFetchScroll, odbcLib, "SQLFetchScroll")
		purego.RegisterLibFunc(&sqlGetData, odbcLib, "SQLGetData")
		purego.RegisterLibFunc(&sqlRowCount, odbcLib, "SQLRowCount")
		purego.RegisterLibFunc(&sqlNumParams, odbcLib, "SQLNumParams")
		purego.RegisterLibFunc(&sqlDescribeParam, odbcLib, "SQLDescribeParam")
		purego.RegisterLibFunc(&sqlGetDiagField, odbcLib, "SQLGetDiagField")
		purego.RegisterLibFunc(&sqlEndTran, odbcLib, "SQLEndTran")
		purego.RegisterLibFunc(&sqlCloseCursor, odbcLib, "SQLCloseCursor")
		purego.RegisterLibFunc(&sqlCancel, odbcLib, "SQLCancel")
		purego.RegisterLibFunc(&sqlFreeStmt, odbcLib, "SQLFreeStmt")
		purego.RegisterLibFunc(&sqlMoreResults, odbcLib, "SQLMoreResults")
		purego.RegisterLibFunc(&sqlSetStmtAttr, odbcLib, "SQLSetStmtAttr")
		purego.RegisterLibFunc(&sqlGetStmtAttr, odbcLib, "SQLGetStmtAttr")
	})
	return initErr
}

// AllocHandle allocates an ODBC handle
func AllocHandle(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN {
	return sqlAllocHandle(handleType, inputHandle, outputHandle)
}

// FreeHandle frees an ODBC handle
func FreeHandle(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN {
	return sqlFreeHandle(handleType, handle)
}

// SetEnvAttr sets an environment attribute
func SetEnvAttr(env SQLHENV, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN {
	return sqlSetEnvAttr(env, attribute, value, stringLength)
}

// DriverConnect connects to a data source using a connection string
func DriverConnect(dbc SQLHDBC, hwnd uintptr, inConnStr string, outConnStr []byte, driverCompletion SQLUSMALLINT) (outLen SQLSMALLINT, ret SQLRETURN) {
	inBytes := append([]byte(inConnStr), 0)
	var outLenPtr SQLSMALLINT
	var outPtr *byte
	var outMax SQLSMALLINT
	if len(outConnStr) > 0 {
		outPtr = &outConnStr[0]
		outMax = SQLSMALLINT(len(outConnStr))
	}
	ret = sqlDriverConnect(dbc, hwnd, &inBytes[0], SQLSMALLINT(SQL_NTS), outPtr, outMax, &outLenPtr, driverCompletion)
	return outLenPtr, ret
}

// Disconnect disconnects from a data source
func Disconnect(dbc SQLHDBC) SQLRETURN {
	return sqlDisconnect(dbc)
}

// SetConnectAttr sets a connection attribute
func SetConnectAttr(dbc SQLHDBC, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN {
	return sqlSetConnectAttr(dbc, attribute, value, stringLength)
}

// GetInfo retrieves driver/data source information
func GetInfo(dbc SQLHDBC, infoType SQLUSMALLINT, infoValue []byte) (stringLength SQLSMALLINT, ret SQLRETURN) {
	var strLen SQLSMALLINT
	ret = sqlGetInfo(dbc, infoType, uintptr(0), 0, &strLen)
	if !IsSuccess(ret) {
		return 0, ret
	}
	if len(infoValue) > 0 {
		ret = sqlGetInfo(dbc, infoType, uintptr(unsafe.Pointer(&infoValue[0])), SQLSMALLINT(len(infoValue)), &strLen)
	}
	return strLen, ret
}

// ExecDirect executes an SQL statement directly
func ExecDirect(stmt SQLHSTMT, query string) SQLRETURN {
	queryBytes := append([]byte(query), 0)
	return sqlExecDirect(stmt, &queryBytes[0], SQLINTEGER(SQL_NTS))
}

// Prepare prepares an SQL statement for execution
func Prepare(stmt SQLHSTMT, query string) SQLRETURN {
	queryBytes := append([]byte(query), 0)
	return sqlPrepare(stmt, &queryBytes[0], SQLINTEGER(SQL_NTS))
}

// Execute executes a prepared statement
func Execute(stmt SQLHSTMT) SQLRETURN {
	return sqlExecute(stmt)
}

// NumResultCols returns the number of columns in a result set
func NumResultCols(stmt SQLHSTMT, columnCount *SQLSMALLINT) SQLRETURN {
	return sqlNumResultCols(stmt, columnCount)
}

// DescribeCol describes a column in a result set
func DescribeCol(stmt SQLHSTMT, colNum SQLUSMALLINT, colName []byte) (nameLen SQLSMALLINT, dataType SQLSMALLINT, colSize SQLULEN, decDigits SQLSMALLINT, nullable SQLSMALLINT, ret SQLRETURN) {
	ret = sqlDescribeCol(stmt, colNum, &colName[0], SQLSMALLINT(len(colName)), &nameLen, &dataType, &colSize, &decDigits, &nullable)
	return
}

// ColAttribute returns a column attribute
func ColAttribute(stmt SQLHSTMT, colNum SQLUSMALLINT, fieldId SQLUSMALLINT, charAttr []byte) (strLen SQLSMALLINT, numAttr SQLLEN, ret SQLRETURN) {
	var charPtr uintptr
	var bufLen SQLSMALLINT
	if len(charAttr) > 0 {
		charPtr = uintptr(unsafe.Pointer(&charAttr[0]))
		bufLen = SQLSMALLINT(len(charAttr))
	}
	ret = sqlColAttribute(stmt, colNum, fieldId, charPtr, bufLen, &strLen, &numAttr)
	return
}

// BindParameter binds a parameter to a statement
func BindParameter(stmt SQLHSTMT, paramNum SQLUSMALLINT, ioType SQLSMALLINT, valueType SQLSMALLINT, paramType SQLSMALLINT, colSize SQLULEN, decDigits SQLSMALLINT, paramValue uintptr, bufferLen SQLLEN, strLenOrInd *SQLLEN) SQLRETURN {
	return sqlBindParameter(stmt, paramNum, ioType, valueType, paramType, colSize, decDigits, paramValue, bufferLen, strLenOrInd)
}

// Fetch fetches the next row from the result set
func Fetch(stmt SQLHSTMT) SQLRETURN {
	return sqlFetch(stmt)
}

// FetchScroll fetches a row from the result set using scroll operations
func FetchScroll(stmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN {
	return sqlFetchScroll(stmt, fetchOrientation, fetchOffset)
}

// GetData retrieves data for a single column
func GetData(stmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue uintptr, bufferLen SQLLEN, strLenOrInd *SQLLEN) SQLRETURN {
	return sqlGetData(stmt, colNum, targetType, targetValue, bufferLen, strLenOrInd)
}

// RowCount returns the number of rows affected by an UPDATE, INSERT, or DELETE
func RowCount(stmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN {
	return sqlRowCount(stmt, rowCount)
}

// NumParams returns the number of parameters in a prepared statement
func NumParams(stmt SQLHSTMT, paramCount *SQLSMALLINT) SQLRETURN {
	return sqlNumParams(stmt, paramCount)
}

// GetDiagRec retrieves diagnostic records
func GetDiagRec(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState []byte, message []byte) (nativeError SQLINTEGER, msgLen SQLSMALLINT, ret SQLRETURN) {
	ret = sqlGetDiagRec(handleType, handle, recNum, &sqlState[0], &nativeError, &message[0], SQLSMALLINT(len(message)), &msgLen)
	return
}

// EndTran commits or rolls back a transaction
func EndTran(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN {
	return sqlEndTran(handleType, handle, completionType)
}

// CloseCursor closes an open cursor
func CloseCursor(stmt SQLHSTMT) SQLRETURN {
	return sqlCloseCursor(stmt)
}

// Cancel cancels a statement execution
func Cancel(stmt SQLHSTMT) SQLRETURN {
	return sqlCancel(stmt)
}

// FreeStmt frees resources associated with a statement
func FreeStmt(stmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN {
	return sqlFreeStmt(stmt, option)
}

// MoreResults checks for more result sets
func MoreResults(stmt SQLHSTMT) SQLRETURN {
	return sqlMoreResults(stmt)
}

// SetStmtAttr sets a statement attribute
func SetStmtAttr(stmt SQLHSTMT, attribute SQLINTEGER, value uintptr, stringLength SQLINTEGER) SQLRETURN {
	return sqlSetStmtAttr(stmt, attribute, value, stringLength)
}
