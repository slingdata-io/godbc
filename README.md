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
| `string` | CHAR, VARCHAR, TEXT |
| `[]byte` | BINARY, VARBINARY, BLOB |
| `time.Time` | DATE, TIME, TIMESTAMP |

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

## Testing with the Basic Example

The `examples/basic` directory contains a test program that validates the driver against any ODBC-compatible database. It creates a test table, inserts rows, validates the data, tests transactions, and cleans up.

### Build

```bash
go build ./examples/basic/
```

### Usage

```bash
./basic -dsn <connection-string> [-schema <schema-name>]
```

### Examples

```bash
# SQL Server
./basic -dsn "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=master;UID=sa;PWD=password;Encrypt=no" -schema dbo

# PostgreSQL
./basic -dsn "Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=postgres;UID=postgres;PWD=password" -schema public

# MySQL
./basic -dsn "Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=test;UID=root;PWD=password"

# SQLite
./basic -dsn "Driver={SQLite3 ODBC Driver};Database=/tmp/test.db"
```

### What It Tests

- Connection and ping
- Table creation with various data types (INTEGER, VARCHAR, FLOAT, BOOLEAN/BIT, TIMESTAMP, BINARY)
- Prepared statement parameter binding
- Data insertion and retrieval
- Value equality validation (verifies inserted values match retrieved values)
- Transaction rollback (inserts row, rolls back, verifies not persisted)
- Transaction commit (inserts row, commits, verifies persisted)
- Table cleanup

The example auto-detects the database type from the DSN and uses appropriate DDL syntax for SQL Server, PostgreSQL, MySQL, SQLite, and Oracle.

## License

MIT License - see LICENSE file
