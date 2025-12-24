// Package main provides a basic example of using the godbc ODBC driver.
// It creates a test table, inserts rows, and selects them back to validate.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/slingdata-io/godbc"
)

// DBType represents the type of database
type DBType int

const (
	DBTypeUnknown DBType = iota
	DBTypeSQLServer
	DBTypePostgres
	DBTypeMySQL
	DBTypeSQLite
	DBTypeOracle
)

func (t DBType) String() string {
	switch t {
	case DBTypeSQLServer:
		return "SQL Server"
	case DBTypePostgres:
		return "PostgreSQL"
	case DBTypeMySQL:
		return "MySQL"
	case DBTypeSQLite:
		return "SQLite"
	case DBTypeOracle:
		return "Oracle"
	default:
		return "Unknown"
	}
}

// detectDBType detects the database type from the DSN
func detectDBType(dsn string) DBType {
	dsnLower := strings.ToLower(dsn)

	// Check for driver names in the DSN
	if strings.Contains(dsnLower, "sql server") ||
		strings.Contains(dsnLower, "sqlserver") ||
		strings.Contains(dsnLower, "sqlncli") ||
		strings.Contains(dsnLower, "msodbcsql") {
		return DBTypeSQLServer
	}
	if strings.Contains(dsnLower, "postgresql") ||
		strings.Contains(dsnLower, "psqlodbc") ||
		strings.Contains(dsnLower, "postgres") {
		return DBTypePostgres
	}
	if strings.Contains(dsnLower, "mysql") ||
		strings.Contains(dsnLower, "mariadb") {
		return DBTypeMySQL
	}
	if strings.Contains(dsnLower, "sqlite") {
		return DBTypeSQLite
	}
	if strings.Contains(dsnLower, "oracle") {
		return DBTypeOracle
	}

	return DBTypeUnknown
}

// DDLTemplates holds DDL templates for different database types
type DDLTemplates struct {
	CreateTable string
	DropTable   string
	// Parameter placeholder style
	ParamStyle string
}

// getDDLTemplates returns DDL templates for the given database type
func getDDLTemplates(dbType DBType, tableName string) DDLTemplates {
	switch dbType {
	case DBTypeSQLServer:
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id INTEGER NOT NULL,
					name VARCHAR(100),
					value FLOAT,
					active BIT,
					created_at DATETIME2,
					data VARBINARY(100),
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE %s", tableName),
			ParamStyle: "?",
		}
	case DBTypePostgres:
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id INTEGER NOT NULL,
					name VARCHAR(100),
					value DOUBLE PRECISION,
					active BOOLEAN,
					created_at TIMESTAMP,
					data BYTEA,
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName),
			ParamStyle: "?",
		}
	case DBTypeMySQL:
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id INTEGER NOT NULL,
					name VARCHAR(100),
					value DOUBLE,
					active TINYINT(1),
					created_at DATETIME(3),
					data VARBINARY(100),
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName),
			ParamStyle: "?",
		}
	case DBTypeSQLite:
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id INTEGER NOT NULL,
					name TEXT,
					value REAL,
					active INTEGER,
					created_at TEXT,
					data BLOB,
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName),
			ParamStyle: "?",
		}
	case DBTypeOracle:
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id NUMBER(10) NOT NULL,
					name VARCHAR2(100),
					value BINARY_DOUBLE,
					active NUMBER(1),
					created_at TIMESTAMP,
					data RAW(100),
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE %s", tableName),
			ParamStyle: "?",
		}
	default:
		// Generic/ANSI SQL fallback
		return DDLTemplates{
			CreateTable: fmt.Sprintf(`
				CREATE TABLE %s (
					id INTEGER NOT NULL,
					name VARCHAR(100),
					value FLOAT,
					active SMALLINT,
					created_at TIMESTAMP,
					data VARBINARY(100),
					PRIMARY KEY (id)
				)`, tableName),
			DropTable:  fmt.Sprintf("DROP TABLE %s", tableName),
			ParamStyle: "?",
		}
	}
}

func main() {
	// Parse command line flags
	dsn := flag.String("conn-string", "", "ODBC connection string (required)")
	schema := flag.String("schema", "", "Schema name for the test table (optional)")
	flag.Parse()

	if *dsn == "" {
		fmt.Println("Usage: basic -conn-string <connection-string> [-schema <schema-name>]")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  basic -conn-string \"DSN=mydsn;UID=user;PWD=password\"")
		fmt.Println("  basic -conn-string \"Driver={PostgreSQL Unicode};Server=localhost;Database=test;UID=user;PWD=pass\" -schema public")
		fmt.Println("  basic -conn-string \"Driver={SQL Server};Server=localhost;Database=test;UID=sa;PWD=pass\" -schema dbo")
		fmt.Println("  basic -conn-string \"Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=test;UID=user;PWD=pass\"")
		os.Exit(1)
	}

	// Detect database type
	dbType := detectDBType(*dsn)
	log.Printf("Detected database type: %s", dbType)

	// Connect to the database
	log.Println("Connecting to database...")
	db, err := sql.Open("odbc", *dsn)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected successfully!")

	// Build table name with optional schema
	tableName := "godbc_test_table"
	if *schema != "" {
		tableName = fmt.Sprintf("%s.%s", *schema, tableName)
	}

	// Get DDL templates for this database type
	ddl := getDDLTemplates(dbType, tableName)

	// Run the test
	if err := runTest(db, dbType, tableName, ddl); err != nil {
		log.Fatalf("Test failed: %v", err)
	}

	log.Println("All tests passed!")
}

func runTest(db *sql.DB, _ DBType, tableName string, ddl DDLTemplates) error {
	// Drop the table if it exists (ignore errors)
	log.Printf("Dropping table %s if exists...", tableName)
	_, _ = db.Exec(ddl.DropTable)

	// Create the test table
	log.Printf("Creating table %s...", tableName)
	if _, err := db.Exec(ddl.CreateTable); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	log.Println("Table created successfully!")

	// Insert test rows
	log.Println("Inserting test rows...")
	testRows := []struct {
		id        int
		name      string
		value     float64
		active    bool
		createdAt time.Time
		data      []byte
	}{
		{1, "Alice", 123.45, true, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), []byte{0x01, 0x02, 0x03}},
		{2, "Bob", 678.90, false, time.Date(2024, 2, 20, 14, 45, 0, 0, time.UTC), []byte{0x04, 0x05, 0x06}},
		{3, "Charlie", 0.0, true, time.Date(2024, 3, 25, 9, 0, 0, 0, time.UTC), nil},
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created_at, data) VALUES (?, ?, ?, ?, ?, ?)", tableName)
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range testRows {
		result, err := stmt.Exec(row.id, row.name, row.value, row.active, row.createdAt, row.data)
		if err != nil {
			return fmt.Errorf("failed to insert row %d: %w", row.id, err)
		}
		affected, _ := result.RowsAffected()
		log.Printf("  Inserted row %d (rows affected: %d)", row.id, affected)
	}

	// Select and validate rows
	log.Println("Selecting rows back...")
	selectSQL := fmt.Sprintf("SELECT id, name, value, active, created_at, data FROM %s ORDER BY id", tableName)
	rows, err := db.Query(selectSQL)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}
	log.Printf("Columns: %v", columns)

	// Fetch and validate rows
	rowCount := 0
	for rows.Next() {
		var id int
		var name sql.NullString
		var value sql.NullFloat64
		var active sql.NullBool
		var createdAt sql.NullTime
		var data []byte

		if err := rows.Scan(&id, &name, &value, &active, &createdAt, &data); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		log.Printf("  Row %d: id=%d, name=%v, value=%v, active=%v, created_at=%v, data=%v",
			rowCount+1, id, name.String, value.Float64, active.Bool, createdAt.Time, data)

		// Validate against expected values
		expected := testRows[rowCount]

		if id != expected.id {
			return fmt.Errorf("row %d: expected id=%d, got %d", rowCount+1, expected.id, id)
		}
		if name.String != expected.name {
			return fmt.Errorf("row %d: expected name=%q, got %q", rowCount+1, expected.name, name.String)
		}
		// Compare floats with tolerance for floating point precision
		if diff := expected.value - value.Float64; diff < -0.001 || diff > 0.001 {
			return fmt.Errorf("row %d: expected value=%v, got %v", rowCount+1, expected.value, value.Float64)
		}
		if active.Bool != expected.active {
			return fmt.Errorf("row %d: expected active=%v, got %v", rowCount+1, expected.active, active.Bool)
		}
		// Compare timestamps (truncate to seconds for cross-database compatibility)
		expectedTime := expected.createdAt.Truncate(time.Second)
		gotTime := createdAt.Time.Truncate(time.Second)
		if !expectedTime.Equal(gotTime) {
			return fmt.Errorf("row %d: expected created_at=%v, got %v", rowCount+1, expectedTime, gotTime)
		}
		// Compare binary data
		if expected.data == nil {
			if len(data) != 0 {
				return fmt.Errorf("row %d: expected data=nil, got %v", rowCount+1, data)
			}
		} else {
			if len(data) != len(expected.data) {
				return fmt.Errorf("row %d: expected data length=%d, got %d", rowCount+1, len(expected.data), len(data))
			}
			for i := range expected.data {
				if data[i] != expected.data[i] {
					return fmt.Errorf("row %d: expected data[%d]=%d, got %d", rowCount+1, i, expected.data[i], data[i])
				}
			}
		}

		log.Printf("  Row %d: ✓ all values match expected", rowCount+1)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	if rowCount != len(testRows) {
		return fmt.Errorf("expected %d rows, got %d", len(testRows), rowCount)
	}
	log.Printf("Retrieved and validated %d rows successfully!", rowCount)

	// Test transaction
	log.Println("Testing transaction...")
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Insert a row in transaction
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (id, name, value, active, created_at) VALUES (?, ?, ?, ?, ?)", tableName),
		4, "David", 999.99, true, time.Now())
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert in transaction: %w", err)
	}

	// Rollback the transaction
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback: %w", err)
	}
	log.Println("Transaction rolled back successfully!")

	// Verify row was not inserted
	var count int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}
	if count != len(testRows) {
		return fmt.Errorf("expected %d rows after rollback, got %d", len(testRows), count)
	}
	log.Printf("Verified rollback: still %d rows", count)

	// Test commit
	log.Println("Testing commit...")
	tx, err = db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (id, name, value, active, created_at) VALUES (?, ?, ?, ?, ?)", tableName),
		5, "Eve", 111.11, false, time.Now())
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert in transaction: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	log.Println("Transaction committed successfully!")

	// Verify row was inserted
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}
	if count != len(testRows)+1 {
		return fmt.Errorf("expected %d rows after commit, got %d", len(testRows)+1, count)
	}
	log.Printf("Verified commit: now %d rows", count)

	// Clean up - drop the table
	log.Printf("Cleaning up: dropping table %s...", tableName)
	if _, err := db.Exec(ddl.DropTable); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	log.Println("Table dropped successfully!")

	return nil
}
