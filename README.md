# godbc

Pure Go ODBC driver for `database/sql` using [purego](https://github.com/ebitengine/purego) for FFI. No CGO required.

## Features

- Pure Go implementation - no CGO required
- Cross-platform: Windows, macOS, Linux
- Standard `database/sql` interface
- Supports prepared statements with parameters
- Transaction support with isolation levels
- Multiple result sets
- Column type information

## Installation

```bash
go get github.com/slingdata-io/godbc
```

## Requirements

You need an ODBC driver manager installed on your system:

- **Windows**: Built-in (`odbc32.dll`)
- **macOS**: Install unixODBC via Homebrew: `brew install unixodbc`
- **Linux**: Install unixODBC: `apt install unixodbc` or `yum install unixODBC`

You also need ODBC drivers for the databases you want to connect to.

## Usage

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/slingdata-io/godbc"
)

func main() {
    // Connect using a DSN
    db, err := sql.Open("odbc", "DSN=mydsn;UID=user;PWD=password")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Or use a DSN-less connection string
    // db, err := sql.Open("odbc", "Driver={SQL Server};Server=localhost;Database=mydb;UID=user;PWD=password")

    // Query
    rows, err := db.Query("SELECT id, name FROM users WHERE active = ?", true)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("ID: %d, Name: %s\n", id, name)
    }
}
```

## Connection String Examples

```go
// DSN-based connection
"DSN=mydsn;UID=user;PWD=password"

// SQL Server (DSN-less)
"Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=mydb;UID=user;PWD=password"

// PostgreSQL (DSN-less)
"Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;UID=user;PWD=password"

// MySQL (DSN-less)
"Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=mydb;UID=user;PWD=password"

// SQLite (DSN-less)
"Driver={SQLite3 ODBC Driver};Database=/path/to/database.db"
```

## Supported Data Types

| Go Type | ODBC SQL Type |
|---------|---------------|
| `bool` | BIT |
| `int8`, `int16`, `int32`, `int64` | TINYINT, SMALLINT, INTEGER, BIGINT |
| `float32`, `float64` | REAL, DOUBLE |
| `string` | CHAR, VARCHAR, TEXT, DECIMAL, NUMERIC |
| `[]byte` | BINARY, VARBINARY, BLOB |
| `time.Time` | DATE, TIME, TIMESTAMP |

## Decimal Precision

DECIMAL and NUMERIC columns are returned as `string` to preserve full precision (avoiding float64 rounding errors). Use the `DecimalSize()` method on column types to get precision and scale metadata:

```go
rows, _ := db.Query("SELECT price FROM products")
defer rows.Close()

cols, _ := rows.ColumnTypes()
for _, col := range cols {
    if prec, scale, ok := col.DecimalSize(); ok {
        fmt.Printf("Column %s: DECIMAL(%d,%d)\n", col.Name(), prec, scale)
    }
}
```

For arbitrary-precision arithmetic, use a decimal library like [shopspring/decimal](https://github.com/shopspring/decimal):

```go
import "github.com/shopspring/decimal"

var priceStr string
rows.Scan(&priceStr)
price, _ := decimal.NewFromString(priceStr)
```

## Transactions

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

_, err = tx.Exec("INSERT INTO users (name) VALUES (?)", "John")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

## Named Parameters

The driver supports named parameters in addition to positional `?` placeholders. Named parameters are automatically converted to positional placeholders before execution.

Supported styles:
- `:name` - Oracle/PostgreSQL style
- `@name` - SQL Server style
- `$name` - PostgreSQL style (not `$1` which is positional)

```go
// Using named parameters
rows, err := db.Query(
    "SELECT * FROM users WHERE name = :name AND status = :status",
    sql.Named("name", "John"),
    sql.Named("status", "active"),
)

// SQL Server style
rows, err := db.Query(
    "SELECT * FROM users WHERE name = @name AND status = @status",
    sql.Named("name", "John"),
    sql.Named("status", "active"),
)
```

Named parameters can appear multiple times in a query:

```go
rows, err := db.Query(
    "SELECT * FROM users WHERE first_name = :name OR last_name = :name",
    sql.Named("name", "Smith"),
)
```

## Connection Options

Use `OpenConnectorWithOptions` for advanced configuration:

```go
import "github.com/slingdata-io/godbc"

// Create connector with options
connector, err := odbc.OpenConnectorWithOptions(
    "Driver={PostgreSQL Unicode};Server=localhost;Database=mydb",
    odbc.WithTimezone(time.UTC),
    odbc.WithTimestampPrecision(odbc.Microseconds),
    odbc.WithQueryTimeout(30 * time.Second),
    odbc.WithLastInsertIdBehavior(odbc.LastInsertIdAuto),
)
if err != nil {
    log.Fatal(err)
}

db := sql.OpenDB(connector)
defer db.Close()
```

### Available Options

| Option | Description |
|--------|-------------|
| `WithTimezone(tz)` | Set timezone for timestamp handling (default: UTC) |
| `WithTimestampPrecision(p)` | Set precision: `Seconds`, `Milliseconds`, `Microseconds`, `Nanoseconds` |
| `WithQueryTimeout(d)` | Set default query timeout (default: no timeout) |
| `WithLastInsertIdBehavior(b)` | Set LastInsertId handling: `LastInsertIdAuto`, `LastInsertIdDisabled` |

## Query Timeout

Set a timeout for query execution:

```go
connector, _ := odbc.OpenConnectorWithOptions(
    connString,
    odbc.WithQueryTimeout(30 * time.Second),
)
db := sql.OpenDB(connector)

// All queries will timeout after 30 seconds
rows, err := db.Query("SELECT * FROM large_table")
```

You can also use context-based timeouts:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
```

## Output Parameters

When calling stored procedures, retrieve output parameter values from the result:

```go
// Execute stored procedure with output parameter
result, err := db.Exec("CALL get_user_count(?)", sql.Out{Dest: new(int64)})
if err != nil {
    log.Fatal(err)
}

// Get output parameters (requires type assertion to *odbc.Result)
if odbcResult, ok := result.(*odbc.Result); ok {
    params := odbcResult.OutputParams()
    count := params[0].(int64)
    fmt.Printf("User count: %d\n", count)
}
```

## Unit Tests

Run the unit tests (no database connection required):

```bash
go test -v
```

The test suite covers:
- Type conversions (Go ↔ ODBC)
- GUID parsing and formatting
- UTF-16 to UTF-8 string conversion
- Error handling utilities
- SQL type name helpers

## Integration Testing with the Basic Example

The `examples/basic` directory contains a test program that validates the driver against any ODBC-compatible database. It creates a test table, inserts rows, validates the data, tests transactions, and cleans up.

### Build

```bash
go build ./examples/basic/
```

### Usage

```bash
./basic -conn-string <connection-string> [-schema <schema-name>]
```

### Examples

```bash
# SQL Server
./basic -conn-string "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=master;UID=sa;PWD=password;Encrypt=no" -schema dbo

# PostgreSQL
./basic -conn-string "Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=postgres;UID=postgres;PWD=password" -schema public

# MySQL
./basic -conn-string "Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=test;UID=root;PWD=password"

# SQLite
./basic -conn-string "Driver={SQLite3 ODBC Driver};Database=/tmp/test.db"
```

### What It Tests

- Connection and ping
- Table creation with various data types (INTEGER, VARCHAR, FLOAT, BOOLEAN/BIT, TIMESTAMP, BINARY, DECIMAL)
- Prepared statement parameter binding
- Data insertion and retrieval
- Value equality validation (verifies inserted values match retrieved values)
- Decimal precision/scale metadata (validates `ColumnTypePrecisionScale` returns correct values)
- Transaction rollback (inserts row, rolls back, verifies not persisted)
- Transaction commit (inserts row, commits, verifies persisted)
- Table cleanup

The example auto-detects the database type from the DSN and uses appropriate DDL syntax for SQL Server, PostgreSQL, MySQL, SQLite, and Oracle.

## Troubleshooting

### ODBC Library Not Found

If you get an error about the ODBC library not being found, set `GODBC_LIBRARY_PATH` to specify a custom library location:

```bash
# macOS
export GODBC_LIBRARY_PATH=/opt/homebrew/lib/libodbc.2.dylib

# Linux
export GODBC_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libodbc.so.2
```

### Known Limitations

- **LastInsertId()**: Always returns 0. ODBC does not have a standard way to retrieve the last inserted ID. Use database-specific queries like `SELECT @@IDENTITY` (SQL Server), `SELECT lastval()` (PostgreSQL), or `SELECT LAST_INSERT_ID()` (MySQL).

- **Timestamp precision**: Timestamps are truncated to millisecond precision for database compatibility. Nanosecond precision is not preserved.

- **DECIMAL/NUMERIC**: Returned as strings to preserve full precision. Use a decimal library like [shopspring/decimal](https://github.com/shopspring/decimal) for arithmetic.

### Error Handling

The driver provides helper functions for error classification:

```go
import "github.com/slingdata-io/godbc"

if err := db.Ping(); err != nil {
    if odbc.IsConnectionError(err) {
        // Handle connection failure
    }
    if odbc.IsRetryable(err) {
        // Retry the operation
    }
}
```

## License

MIT License - see LICENSE file
