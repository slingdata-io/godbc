package odbc

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

// Connector implements driver.Connector for efficient connection pooling
type Connector struct {
	dsn    string
	driver *Driver

	// Enhanced Type Handling options
	DefaultTimezone           *time.Location       // Default timezone for timestamp retrieval (defaults to UTC)
	DefaultTimestampPrecision TimestampPrecision   // Default precision for Timestamp type (defaults to Milliseconds)
	LastInsertIdBehavior      LastInsertIdBehavior // How to handle LastInsertId() (defaults to Auto)

	// Query execution options
	QueryTimeout time.Duration // Default query timeout (0 = no timeout)
}

// ConnectorOption configures a Connector
type ConnectorOption func(*Connector)

// WithTimezone sets the default timezone for timestamp handling
func WithTimezone(tz *time.Location) ConnectorOption {
	return func(c *Connector) {
		c.DefaultTimezone = tz
	}
}

// WithTimestampPrecision sets the default timestamp precision
func WithTimestampPrecision(precision TimestampPrecision) ConnectorOption {
	return func(c *Connector) {
		c.DefaultTimestampPrecision = precision
	}
}

// WithLastInsertIdBehavior sets the behavior for LastInsertId()
func WithLastInsertIdBehavior(behavior LastInsertIdBehavior) ConnectorOption {
	return func(c *Connector) {
		c.LastInsertIdBehavior = behavior
	}
}

// WithQueryTimeout sets the default query timeout for all statements.
// The timeout is applied using SQL_ATTR_QUERY_TIMEOUT and context cancellation.
// A value of 0 means no timeout (the default).
func WithQueryTimeout(d time.Duration) ConnectorOption {
	return func(c *Connector) {
		c.QueryTimeout = d
	}
}

// Connect establishes a new connection to the database
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Allocate environment handle
	var env SQLHENV
	ret := AllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (*SQLHANDLE)(&env))
	if !IsSuccess(ret) {
		return nil, errors.New("failed to allocate ODBC environment handle")
	}

	// Set ODBC version to 3.x
	ret = SetEnvAttr(env, SQL_ATTR_ODBC_VERSION, uintptr(SQL_OV_ODBC3), 0)
	if !IsSuccess(ret) {
		FreeHandle(SQL_HANDLE_ENV, SQLHANDLE(env))
		return nil, NewError(SQL_HANDLE_ENV, SQLHANDLE(env))
	}

	// Allocate connection handle
	var dbc SQLHDBC
	ret = AllocHandle(SQL_HANDLE_DBC, SQLHANDLE(env), (*SQLHANDLE)(&dbc))
	if !IsSuccess(ret) {
		err := NewError(SQL_HANDLE_ENV, SQLHANDLE(env))
		FreeHandle(SQL_HANDLE_ENV, SQLHANDLE(env))
		return nil, err
	}

	// Connect using the connection string
	outConnStr := make([]byte, 1024)
	_, ret = DriverConnect(dbc, 0, c.dsn, outConnStr, SQL_DRIVER_NOPROMPT)
	if !IsSuccess(ret) {
		err := NewError(SQL_HANDLE_DBC, SQLHANDLE(dbc))
		FreeHandle(SQL_HANDLE_DBC, SQLHANDLE(dbc))
		FreeHandle(SQL_HANDLE_ENV, SQLHANDLE(env))
		return nil, err
	}

	// Create and return the connection
	conn := &Conn{
		env:                  env,
		dbc:                  dbc,
		lastInsertIdBehavior: c.LastInsertIdBehavior,
		queryTimeout:         c.QueryTimeout,
	}

	// Detect database type for LastInsertId support
	if conn.lastInsertIdBehavior == LastInsertIdAuto {
		conn.detectDatabaseType()
	}

	return conn, nil
}

// Driver returns the underlying Driver
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Ensure Connector implements driver.Connector
var _ driver.Connector = (*Connector)(nil)
