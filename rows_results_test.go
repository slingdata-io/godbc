package godbc

import (
	"context"
	"errors"
	"io"
	"testing"

	"database/sql/driver"
)

func newTestStmt() *Stmt { return &Stmt{conn: newFakeConn(), stmt: 1} }

func expectCalls(t *testing.T, f *fakeODBC, name string, want int) {
	t.Helper()
	if got := f.called(name); got != want {
		t.Errorf("%s calls = %d, want %d", name, got, want)
	}
}

func expectNext(t *testing.T, r *Rows, want string) {
	t.Helper()
	dest := make([]driver.Value, len(r.Columns()))
	if err := r.Next(dest); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if dest[0] != want {
		t.Errorf("value = %v, want %q", dest[0], want)
	}
}

func expectEOF(t *testing.T, r *Rows) {
	t.Helper()
	if err := r.Next(make([]driver.Value, 1)); err != io.EOF {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
}

func TestNewRows_NoColumns(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{{}}}
	installFakeODBC(t, f)

	r, err := newRows(newTestStmt(), false)
	if err != nil {
		t.Fatalf("newRows: %v", err)
	}
	if len(r.Columns()) != 0 {
		t.Errorf("columns = %v, want none", r.Columns())
	}
	expectEOF(t, r)
	expectCalls(t, f, "Fetch", 0)
	if r.HasNextResultSet() {
		t.Error("HasNextResultSet = true, want false")
	}
	expectCalls(t, f, "MoreResults", 1)
}

func TestNewRows_SkipsNoColumnResults(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{
		{},
		{},
		{cols: []string{"n"}, rows: [][]string{{"2"}}},
	}}
	installFakeODBC(t, f)

	r, err := newRows(newTestStmt(), false)
	if err != nil {
		t.Fatalf("newRows: %v", err)
	}
	expectCalls(t, f, "MoreResults", 2)
	if got := r.Columns(); len(got) != 1 || got[0] != "n" {
		t.Fatalf("columns = %v, want [n]", got)
	}
	expectNext(t, r, "2")
	expectEOF(t, r)
}

func TestNewRows_MoreResultsError(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{{}, {moreErr: "42S02"}}}
	installFakeODBC(t, f)

	r, err := newRows(newTestStmt(), false)
	if r != nil {
		t.Error("rows is not nil")
	}
	var odbcErr *Error
	if !errors.As(err, &odbcErr) || odbcErr.SQLState != "42S02" {
		t.Fatalf("err = %v, want *Error 42S02", err)
	}
	expectCalls(t, f, "FreeStmtClose", 1)
}

func TestNewRows_MoreResultsNotSupported(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{{}, {moreErr: "IM001"}}}
	installFakeODBC(t, f)

	r, err := newRows(newTestStmt(), false)
	if err != nil {
		t.Fatalf("newRows: %v", err)
	}
	if len(r.Columns()) != 0 {
		t.Errorf("columns = %v, want none", r.Columns())
	}
	expectEOF(t, r)
	if r.HasNextResultSet() {
		t.Error("HasNextResultSet = true, want false")
	}
}

func TestRows_NextResultSetSkipsNoColumnResults(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{
		{cols: []string{"a"}},
		{},
		{cols: []string{"b"}},
	}}
	installFakeODBC(t, f)

	r, err := newRows(newTestStmt(), false)
	if err != nil {
		t.Fatalf("newRows: %v", err)
	}
	if err := r.NextResultSet(); err != nil {
		t.Fatalf("NextResultSet: %v", err)
	}
	if got := r.Columns(); len(got) != 1 || got[0] != "b" {
		t.Errorf("columns = %v, want [b]", got)
	}
	if err := r.NextResultSet(); err != io.EOF {
		t.Errorf("NextResultSet = %v, want io.EOF", err)
	}
}

func TestConnQueryContext_NoData(t *testing.T) {
	f := &fakeODBC{execRet: SQL_NO_DATA, results: []fakeResult{{}}}
	installFakeODBC(t, f)

	rows, err := newFakeConn().QueryContext(context.Background(), "delete from t where 1=0", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()
	if err := rows.Next(make([]driver.Value, 1)); err != io.EOF {
		t.Errorf("Next = %v, want io.EOF", err)
	}
}

func TestStmtQueryContext_NoData(t *testing.T) {
	f := &fakeODBC{execRet: SQL_NO_DATA, results: []fakeResult{{}}}
	installFakeODBC(t, f)

	rows, err := newTestStmt().QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if err := rows.Next(make([]driver.Value, 1)); err != io.EOF {
		t.Errorf("Next = %v, want io.EOF", err)
	}
}

func TestConnQueryContext_FreesHandleOnError(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{{}, {moreErr: "42S02"}}}
	installFakeODBC(t, f)

	rows, err := newFakeConn().QueryContext(context.Background(), "select 1", nil)
	if err == nil {
		rows.Close()
		t.Fatal("QueryContext returned no error")
	}
	expectCalls(t, f, "FreeStmtHandle", 1)
}

func TestDB_InsertThenSelect(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{
		{},
		{cols: []string{"n"}, rows: [][]string{{"3"}}},
	}}
	db := openFakeDB(t, f)

	rows, err := db.Query("insert into t values (3); select count(*) as n from t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 1 || got[0] != "3" {
		t.Errorf("rows = %v, want [3]", got)
	}
}

func TestDB_NextResultSetNoDoubleAdvance(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{
		{cols: []string{"a"}, rows: [][]string{{"1"}}},
		{cols: []string{"b"}, rows: [][]string{{"2"}}},
	}}
	db := openFakeDB(t, f)

	rows, err := db.Query("select 1 as a; select 2 as b")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
	}
	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet = false, err = %v", rows.Err())
	}
	expectCalls(t, f, "MoreResults", 1)
	cols, _ := rows.Columns()
	if len(cols) != 1 || cols[0] != "b" {
		t.Fatalf("columns = %v, want [b]", cols)
	}
	if !rows.Next() {
		t.Fatalf("Next = false, err = %v", rows.Err())
	}
	var b string
	if err := rows.Scan(&b); err != nil || b != "2" {
		t.Errorf("Scan = %q, %v, want 2", b, err)
	}
	if rows.Next() {
		t.Error("extra row")
	}
	if rows.NextResultSet() {
		t.Error("NextResultSet = true at the end")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("Err: %v", err)
	}
}

func TestDB_LaterStatementError(t *testing.T) {
	f := &fakeODBC{results: []fakeResult{
		{cols: []string{"a"}, rows: [][]string{{"1"}}},
		{moreErr: "42S02"},
	}}
	db := openFakeDB(t, f)

	rows, err := db.Query("select 1 as a; insert into missing values (1)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("first Next = false, err = %v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("second Next = true")
	}
	var odbcErr *Error
	if err := rows.Err(); !errors.As(err, &odbcErr) || odbcErr.SQLState != "42S02" {
		t.Fatalf("Err = %v, want *Error 42S02", err)
	}
}

func TestHasSQLState(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"error", &Error{SQLState: "IM001"}, true},
		{"errors with match", Errors{{SQLState: "01000"}, {SQLState: "IM001"}}, true},
		{"errors without match", Errors{{SQLState: "01000"}}, false},
		{"other error", errors.New("IM001"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		if got := hasSQLState(tt.err, "IM001"); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestRows_ScrollNoColumns(t *testing.T) {
	f := &fakeODBC{}
	installFakeODBC(t, f)
	r := &Rows{stmt: newTestStmt()}

	calls := map[string]func() error{
		"First":    r.First,
		"Last":     r.Last,
		"Prior":    r.Prior,
		"Absolute": func() error { return r.Absolute(1) },
		"Relative": func() error { return r.Relative(1) },
	}
	for name, call := range calls {
		if err := call(); err != io.EOF {
			t.Errorf("%s = %v, want io.EOF", name, err)
		}
	}
	expectCalls(t, f, "FetchScroll", 0)
}
