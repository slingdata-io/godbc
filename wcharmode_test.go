package godbc

import (
	"bytes"
	"reflect"
	"testing"
	"unsafe"
)

func TestParseWCharMode(t *testing.T) {
	tests := []struct {
		in   string
		want wcharMode
	}{
		{"", wcharModeAuto},
		{"auto", wcharModeAuto},
		{"AUTO", wcharModeAuto},
		{" utf16 ", wcharModeUTF16},
		{"wide", wcharModeUTF16},
		{"char", wcharModeChar},
		{"UTF8", wcharModeChar},
		{"x", wcharModeAuto},
	}
	for _, tt := range tests {
		if got := parseWCharMode(tt.in); got != tt.want {
			t.Errorf("parseWCharMode(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestResolveWCharMode(t *testing.T) {
	tests := []struct {
		setting wcharMode
		goos    string
		driver  string
		want    wcharMode
	}{
		{wcharModeAuto, "linux", "OdbcFb", wcharModeChar},
		{wcharModeAuto, "darwin", "libOdbcFb.so", wcharModeChar},
		{wcharModeAuto, "windows", "OdbcFb", wcharModeUTF16},
		{wcharModeAuto, "linux", "libmsodbcsql-18.so", wcharModeUTF16},
		{wcharModeAuto, "linux", "", wcharModeUTF16},
		{wcharModeUTF16, "linux", "OdbcFb", wcharModeUTF16},
		{wcharModeChar, "windows", "x", wcharModeChar},
	}
	for _, tt := range tests {
		if got := resolveWCharMode(tt.setting, tt.goos, tt.driver); got != tt.want {
			t.Errorf("resolveWCharMode(%d, %s, %q) = %d, want %d",
				tt.setting, tt.goos, tt.driver, got, tt.want)
		}
	}
}

func TestConnGetInfoString(t *testing.T) {
	orig := sqlGetInfo
	t.Cleanup(func() { sqlGetInfo = orig })

	ret := SQL_SUCCESS
	var calls []SQLSMALLINT
	sqlGetInfo = func(dbc SQLHDBC, infoType SQLUSMALLINT, infoValue unsafe.Pointer, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN {
		calls = append(calls, bufLen)
		if ret != SQL_SUCCESS {
			return ret
		}
		*strLen = 6
		if bufLen != 0 {
			copy(fakeBytes(infoValue, int(bufLen)), "OdbcFb\x00")
		}
		return SQL_SUCCESS
	}

	c := newFakeConn()
	if got := c.getInfoString(SQL_DRIVER_NAME); got != "OdbcFb" {
		t.Errorf("getInfoString = %q, want OdbcFb", got)
	}
	if len(calls) != 2 || calls[0] != 0 || calls[1] != 256 {
		t.Errorf("sqlGetInfo buffer lengths = %v, want [0 256]", calls)
	}

	ret = SQL_ERROR
	if got := c.getInfoString(SQL_DRIVER_NAME); got != "" {
		t.Errorf("getInfoString on error = %q, want empty", got)
	}
}

func TestConvertToODBCWithMode_Char(t *testing.T) {
	const text = "é日😀"
	wantBytes := []byte{0xc3, 0xa9, 0xe6, 0x97, 0xa5, 0xf0, 0x9f, 0x98, 0x80, 0}

	for _, v := range []interface{}{text, WideString(text)} {
		buf, cType, sqlType, colSize, dec, length, err := convertToODBCWithMode(v, wcharModeChar)
		if err != nil {
			t.Fatalf("%T: %v", v, err)
		}
		if b, ok := buf.([]byte); !ok || !bytes.Equal(b, wantBytes) {
			t.Errorf("%T: buf = %v, want %v", v, buf, wantBytes)
		}
		if cType != SQL_C_CHAR || sqlType != SQL_VARCHAR || colSize != 9 || dec != 0 || length != 9 {
			t.Errorf("%T: got ctype %d sqltype %d colSize %d dec %d len %d", v, cType, sqlType, colSize, dec, length)
		}
	}

	_, _, _, colSize, _, length, err := convertToODBCWithMode("", wcharModeChar)
	if err != nil || colSize != 1 || length != 0 {
		t.Errorf("empty string: colSize %d len %d err %v", colSize, length, err)
	}

	for _, v := range []interface{}{int64(5), nil} {
		gb, gc, gs, gz, gd, gl, _ := convertToODBCWithMode(v, wcharModeChar)
		wb, wc, ws, wz, wd, wl, _ := convertToODBC(v)
		if !reflect.DeepEqual(gb, wb) || gc != wc || gs != ws || gz != wz || gd != wd || gl != wl {
			t.Errorf("%T: result differs from convertToODBC", v)
		}
	}
}

func TestConvertToODBCWithMode_UTF16(t *testing.T) {
	const text = "é日😀"
	for _, mode := range []wcharMode{wcharModeAuto, wcharModeUTF16} {
		buf, cType, sqlType, colSize, _, length, err := convertToODBCWithMode(text, mode)
		if err != nil {
			t.Fatal(err)
		}
		wbuf, wcType, wsqlType, wcolSize, _, wlength, _ := convertToODBC(text)
		if !reflect.DeepEqual(buf, wbuf) || cType != wcType || sqlType != wsqlType || colSize != wcolSize || length != wlength {
			t.Errorf("mode %d differs from convertToODBC", mode)
		}
		if _, ok := buf.([]uint16); !ok || cType != SQL_C_WCHAR {
			t.Errorf("mode %d: want []uint16 and SQL_C_WCHAR, got %T %d", mode, buf, cType)
		}
	}
}

func TestAllocateColumnArray_StringsCharMode(t *testing.T) {
	values := []interface{}{"hello", "é日😀", nil}
	cb, err := allocateColumnArray(values, 3, wcharModeChar)
	if err != nil {
		t.Fatal(err)
	}
	if cb.CType != SQL_C_CHAR || cb.SQLType != SQL_VARCHAR {
		t.Errorf("types = %d %d", cb.CType, cb.SQLType)
	}
	if cb.ElemSize != 10 || cb.ColSize != 9 {
		t.Errorf("ElemSize %d ColSize %d, want 10 and 9", cb.ElemSize, cb.ColSize)
	}
	wantLens := []SQLLEN{5, 9, SQL_NULL_DATA}
	if !reflect.DeepEqual(cb.Lengths, wantLens) {
		t.Errorf("Lengths = %v, want %v", cb.Lengths, wantLens)
	}
	data := cb.Data.([]byte)
	if !bytes.Equal(data[10:19], []byte("é日😀")) || data[19] != 0 {
		t.Errorf("data at offset 10 = %v", data[10:20])
	}

	cb, err = allocateColumnArray([]interface{}{"", nil}, 2, wcharModeChar)
	if err != nil {
		t.Fatal(err)
	}
	if cb.ColSize != 255 || cb.ElemSize != 256 {
		t.Errorf("empty: ColSize %d ElemSize %d, want 255 and 256", cb.ColSize, cb.ElemSize)
	}
}

func TestRowsWideColumnCharMode(t *testing.T) {
	orig := sqlGetData
	t.Cleanup(func() { sqlGetData = orig })

	var gotType SQLSMALLINT
	var gotLen SQLLEN
	sqlGetData = func(stmt SQLHSTMT, col SQLUSMALLINT, ctype SQLSMALLINT, target unsafe.Pointer, bufLen SQLLEN, ind *SQLLEN) SQLRETURN {
		gotType, gotLen = ctype, bufLen
		if ctype == SQL_C_CHAR {
			const val = "é日😀"
			copy(fakeBytes(target, int(bufLen)), val+"\x00")
			*ind = SQLLEN(len(val))
		} else {
			// "é日😀" as UTF-16
			u := stringToUTF16("é日😀")
			b := fakeBytes(target, int(bufLen))
			for i, c := range u {
				b[i*2], b[i*2+1] = byte(c), byte(c>>8)
			}
			*ind = SQLLEN((len(u) - 1) * 2)
		}
		return SQL_SUCCESS
	}

	newRows := func(mode wcharMode) *Rows {
		return &Rows{
			stmt:     &Stmt{conn: &Conn{wcharMode: mode}},
			colTypes: []SQLSMALLINT{SQL_WVARCHAR},
			colSizes: []SQLULEN{3},
		}
	}

	got, err := newRows(wcharModeChar).getColumnData(1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "é日😀" {
		t.Errorf("char mode value = %v", got)
	}
	if gotType != SQL_C_CHAR || gotLen < 256 {
		t.Errorf("char mode: ctype %d bufLen %d", gotType, gotLen)
	}

	got, err = newRows(wcharModeUTF16).getColumnData(1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "é日😀" {
		t.Errorf("utf16 mode value = %v", got)
	}
	if gotType != SQL_C_WCHAR {
		t.Errorf("utf16 mode: ctype %d, want SQL_C_WCHAR", gotType)
	}
}

func TestStmtBindParamCharMode(t *testing.T) {
	orig := sqlBindParameter
	t.Cleanup(func() { sqlBindParameter = orig })

	var valueType, paramType SQLSMALLINT
	var bufLen SQLLEN
	sqlBindParameter = func(stmt SQLHSTMT, n SQLUSMALLINT, io, vt, pt SQLSMALLINT, colSize SQLULEN, dec SQLSMALLINT, p unsafe.Pointer, bl SQLLEN, ind *SQLLEN) SQLRETURN {
		valueType, paramType, bufLen = vt, pt, bl
		return SQL_SUCCESS
	}

	s := &Stmt{conn: &Conn{wcharMode: wcharModeChar}}
	if err := s.bindParam(1, "é日😀"); err != nil {
		t.Fatal(err)
	}
	if valueType != SQL_C_CHAR || paramType != SQL_VARCHAR || bufLen != 10 || *s.paramLengths[0] != 9 {
		t.Errorf("char: valueType %d paramType %d bufLen %d len %d", valueType, paramType, bufLen, *s.paramLengths[0])
	}

	s = &Stmt{conn: &Conn{wcharMode: wcharModeUTF16}}
	if err := s.bindParam(1, "é日😀"); err != nil {
		t.Fatal(err)
	}
	if valueType != SQL_C_WCHAR || paramType != SQL_WVARCHAR {
		t.Errorf("utf16: valueType %d paramType %d", valueType, paramType)
	}
}
