package godbc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"unsafe"
)

// fakeResult is one result of a fake batch.
type fakeResult struct {
	cols    []string
	types   []SQLSMALLINT // default SQL_VARCHAR
	rows    [][]string
	moreErr string // SQLSTATE: SQLMoreResults fails when it moves to this result
}

// fakeODBC is an in-memory ODBC driver behind the FFI function variables.
type fakeODBC struct {
	results []fakeResult
	execRet SQLRETURN // return code of SQLExecDirect and SQLExecute

	mu    sync.Mutex
	cur   int // index of the current result
	row   int // number of rows fetched from the current result
	diag  string
	calls map[string]int
}

func (f *fakeODBC) count(name string) {
	f.mu.Lock()
	f.calls[name]++
	f.mu.Unlock()
}

func (f *fakeODBC) called(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

func (f *fakeODBC) fail(state string) SQLRETURN {
	f.diag = state
	return SQL_ERROR
}

func (f *fakeODBC) current() *fakeResult {
	if f.cur < 0 || f.cur >= len(f.results) {
		return nil
	}
	return &f.results[f.cur]
}

// fakeBytes turns an ODBC buffer address into a slice.
func fakeBytes(p unsafe.Pointer, n int) []byte {
	return unsafe.Slice((*byte)(p), n)
}

// installFakeODBC replaces the FFI functions with stubs that read f.
func installFakeODBC(t *testing.T, f *fakeODBC) {
	t.Helper()
	if f.calls == nil {
		f.calls = map[string]int{}
	}

	// Make ExecDirect use sqlExecDirect.
	t.Setenv("GODBC_EXEC_MODE", "")
	origOverride := execModeOverride.Load()
	execModeOverride.Store(0)

	origAllocHandle := sqlAllocHandle
	origFreeHandle := sqlFreeHandle
	origExecDirect := sqlExecDirect
	origExecute := sqlExecute
	origNumResultCols := sqlNumResultCols
	origMoreResults := sqlMoreResults
	origFetch := sqlFetch
	origFetchScroll := sqlFetchScroll
	origDescribeCol := sqlDescribeCol
	origColAttribute := sqlColAttribute
	origGetData := sqlGetData
	origGetDiagRec := sqlGetDiagRec
	origCloseCursor := sqlCloseCursor
	origFreeStmt := sqlFreeStmt
	origDisconnect := sqlDisconnect
	origCancel := sqlCancel
	t.Cleanup(func() {
		sqlAllocHandle = origAllocHandle
		sqlFreeHandle = origFreeHandle
		sqlExecDirect = origExecDirect
		sqlExecute = origExecute
		sqlNumResultCols = origNumResultCols
		sqlMoreResults = origMoreResults
		sqlFetch = origFetch
		sqlFetchScroll = origFetchScroll
		sqlDescribeCol = origDescribeCol
		sqlColAttribute = origColAttribute
		sqlGetData = origGetData
		sqlGetDiagRec = origGetDiagRec
		sqlCloseCursor = origCloseCursor
		sqlFreeStmt = origFreeStmt
		sqlDisconnect = origDisconnect
		sqlCancel = origCancel
		execModeOverride.Store(origOverride)
	})

	nextHandle := SQLHANDLE(100)
	sqlAllocHandle = func(ht SQLSMALLINT, in SQLHANDLE, out *SQLHANDLE) SQLRETURN {
		f.count("AllocHandle")
		f.mu.Lock()
		nextHandle++
		*out = nextHandle
		f.mu.Unlock()
		return SQL_SUCCESS
	}
	sqlFreeHandle = func(ht SQLSMALLINT, h SQLHANDLE) SQLRETURN {
		if ht == SQL_HANDLE_STMT {
			f.count("FreeStmtHandle")
		}
		return SQL_SUCCESS
	}
	sqlExecDirect = func(stmt SQLHSTMT, text *byte, n SQLINTEGER) SQLRETURN {
		f.count("ExecDirect")
		return f.execRet
	}
	sqlExecute = func(stmt SQLHSTMT) SQLRETURN {
		f.count("Execute")
		return f.execRet
	}
	sqlNumResultCols = func(stmt SQLHSTMT, n *SQLSMALLINT) SQLRETURN {
		f.count("NumResultCols")
		*n = 0
		if res := f.current(); res != nil {
			*n = SQLSMALLINT(len(res.cols))
		}
		return SQL_SUCCESS
	}
	sqlMoreResults = func(stmt SQLHSTMT) SQLRETURN {
		f.count("MoreResults")
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.cur+1 >= len(f.results) {
			f.cur = len(f.results)
			return SQL_NO_DATA
		}
		f.cur++
		f.row = 0
		if state := f.results[f.cur].moreErr; state != "" {
			return f.fail(state)
		}
		return SQL_SUCCESS
	}
	sqlFetch = func(stmt SQLHSTMT) SQLRETURN {
		f.count("Fetch")
		f.mu.Lock()
		defer f.mu.Unlock()
		res := f.current()
		if res == nil || len(res.cols) == 0 {
			return f.fail(SQLStateInvalidCursorState)
		}
		if f.row >= len(res.rows) {
			return SQL_NO_DATA
		}
		f.row++
		return SQL_SUCCESS
	}
	sqlFetchScroll = func(stmt SQLHSTMT, orient SQLSMALLINT, offset SQLLEN) SQLRETURN {
		f.count("FetchScroll")
		return f.fail(SQLStateInvalidCursorState)
	}
	sqlDescribeCol = func(stmt SQLHSTMT, col SQLUSMALLINT, name *byte, bufLen SQLSMALLINT, nameLen *SQLSMALLINT, dataType *SQLSMALLINT, colSize *SQLULEN, dec *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN {
		f.count("DescribeCol")
		res := f.current()
		i := int(col) - 1
		if res == nil || i < 0 || i >= len(res.cols) {
			return f.fail("07009")
		}
		copy(unsafe.Slice(name, int(bufLen)), res.cols[i])
		*nameLen = SQLSMALLINT(len(res.cols[i]))
		*dataType = SQL_VARCHAR
		if i < len(res.types) {
			*dataType = res.types[i]
		}
		*colSize = 20
		return SQL_SUCCESS
	}
	sqlColAttribute = func(stmt SQLHSTMT, col SQLUSMALLINT, field SQLUSMALLINT, attr unsafe.Pointer, bufLen SQLSMALLINT, strLen *SQLSMALLINT, num *SQLLEN) SQLRETURN {
		*strLen = 0
		return SQL_SUCCESS
	}
	sqlGetData = func(stmt SQLHSTMT, col SQLUSMALLINT, ctype SQLSMALLINT, target unsafe.Pointer, bufLen SQLLEN, ind *SQLLEN) SQLRETURN {
		f.count("GetData")
		f.mu.Lock()
		defer f.mu.Unlock()
		res := f.current()
		if res == nil || f.row < 1 || f.row > len(res.rows) || ctype != SQL_C_CHAR {
			return f.fail("HY000")
		}
		val := res.rows[f.row-1][col-1]
		copy(fakeBytes(target, int(bufLen)), val+"\x00")
		*ind = SQLLEN(len(val))
		return SQL_SUCCESS
	}
	sqlGetDiagRec = func(ht SQLSMALLINT, h SQLHANDLE, rec SQLSMALLINT, state *byte, native *SQLINTEGER, msg *byte, bufLen SQLSMALLINT, msgLen *SQLSMALLINT) SQLRETURN {
		f.mu.Lock()
		defer f.mu.Unlock()
		if rec != 1 || f.diag == "" {
			return SQL_NO_DATA
		}
		copy(unsafe.Slice(state, 6), f.diag+"\x00")
		const text = "fake error"
		copy(unsafe.Slice(msg, int(bufLen)), text)
		*msgLen = SQLSMALLINT(len(text))
		return SQL_SUCCESS
	}
	sqlCloseCursor = func(stmt SQLHSTMT) SQLRETURN {
		f.count("CloseCursor")
		return SQL_SUCCESS
	}
	sqlFreeStmt = func(stmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN {
		if option == SQL_CLOSE {
			f.count("FreeStmtClose")
		}
		return SQL_SUCCESS
	}
	sqlDisconnect = func(dbc SQLHDBC) SQLRETURN { return SQL_SUCCESS }
	sqlCancel = func(stmt SQLHSTMT) SQLRETURN { return SQL_SUCCESS }
}

// fakeConnector gives database/sql one fixed Conn.
type fakeConnector struct{ conn *Conn }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c fakeConnector) Driver() driver.Driver                        { return &Driver{} }

// newFakeConn returns a Conn with a non-zero dbc, so that IsValid is true.
func newFakeConn() *Conn { return &Conn{dbc: 1} }

// openFakeDB installs the fake and opens a sql.DB with one connection.
func openFakeDB(t *testing.T, f *fakeODBC) *sql.DB {
	t.Helper()
	installFakeODBC(t, f)
	db := sql.OpenDB(fakeConnector{conn: newFakeConn()})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}
