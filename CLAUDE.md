# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

godbc is a pure Go ODBC driver for `database/sql` that uses [purego](https://github.com/ebitengine/purego) for FFI instead of CGO. It provides cross-platform ODBC access on Windows, macOS, and Linux.

## Build & Test Commands

```bash
# Build the library
go build .

# Build the test example
go build ./examples/basic/

# Run the test example against a database
./basic -conn-string "Driver={PostgreSQL Unicode};Server=localhost;Database=test;UID=user;PWD=pass" -schema public
./basic -conn-string "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=master;UID=sa;PWD=pass;Encrypt=no" -schema dbo
./basic -conn-string "Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=test;UID=root;PWD=pass"
./basic -conn-string "Driver={SQLite3 ODBC Driver};Database=/tmp/test.db"
```

## Architecture

The driver implements Go's `database/sql/driver` interfaces using ODBC via purego FFI:

```
driver.go      → Driver, registers "odbc" with database/sql
connector.go   → Connector, handles connection establishment
conn.go        → Conn, database connection with Prepare/Exec/Query/BeginTx
stmt.go        → Stmt, prepared statements with parameter binding
rows.go        → Rows, result set iteration with type metadata
tx.go          → Tx, transaction commit/rollback
result.go      → Result, rows affected from INSERT/UPDATE/DELETE
```

ODBC layer (FFI via purego):
```
odbc.go          → ODBC function wrappers, library initialization
odbc_unix.go     → Unix library loading (purego.Dlopen)
odbc_windows.go  → Windows library loading (syscall.LoadLibrary)
types.go         → ODBC constants and struct types (handles, SQL types)
convert.go       → Go ↔ ODBC type conversions for parameter binding
errors.go        → ODBC diagnostic record retrieval and error types
```

## Key Implementation Details

- **Library Loading**: Platform-specific in `odbc_*.go`. Windows uses `odbc32.dll`, macOS searches Homebrew paths for `libodbc.2.dylib`, Linux uses `libodbc.so.2`.

- **Function Registration**: Windows uses ANSI function variants (`SQLDriverConnectA`, `SQLExecDirectA`, etc.) while Unix uses standard names.

- **Parameter Binding**: `convert.go` handles Go→ODBC type conversion. Parameters are bound via `SQLBindParameter` with buffers kept alive in `Stmt.paramBuffers`.

- **Data Retrieval**: `rows.go` uses `SQLGetData` with type-specific methods (`getBool`, `getInt64`, `getString`, etc.). Large strings/blobs handle truncation via multiple fetch calls.

- **DECIMAL/NUMERIC**: Retrieved as strings to preserve precision. `ColumnTypePrecisionScale()` returns precision/scale metadata.

- **Timestamps**: Converted with millisecond precision for database compatibility (via `SQL_TIMESTAMP_STRUCT`).

## Interface Compliance

The driver implements these `database/sql/driver` interfaces:
- `Driver`, `DriverContext` (driver.go)
- `Connector` (connector.go)
- `Conn`, `ConnPrepareContext`, `ConnBeginTx`, `Pinger`, `ExecerContext`, `QueryerContext`, `SessionResetter`, `Validator` (conn.go)
- `Stmt`, `StmtExecContext`, `StmtQueryContext` (stmt.go)
- `Rows`, `RowsColumnTypeScanType`, `RowsColumnTypeDatabaseTypeName`, `RowsColumnTypeLength`, `RowsColumnTypeNullable`, `RowsColumnTypePrecisionScale`, `RowsNextResultSet` (rows.go)
- `Tx` (tx.go)
- `Result` (result.go)

## Requirements

- ODBC driver manager: Windows (built-in), macOS (`brew install unixodbc`), Linux (`apt install unixodbc`)
- ODBC drivers for target databases (SQL Server, PostgreSQL, MySQL, SQLite, etc.)
