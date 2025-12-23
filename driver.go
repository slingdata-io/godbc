package odbc

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("odbc", &Driver{})
}

// Driver implements the database/sql/driver.Driver interface
type Driver struct{}

// Open opens a new connection to the database
// The name is an ODBC connection string, e.g.:
//   - "DSN=mydsn;UID=user;PWD=password"
//   - "Driver={SQL Server};Server=localhost;Database=mydb;UID=user;PWD=password"
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector returns a new Connector for the given connection string
// This implements driver.DriverContext for connection pooling efficiency
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// Initialize ODBC library if not already done
	if err := initODBC(); err != nil {
		return nil, err
	}
	return &Connector{dsn: name, driver: d}, nil
}

// Ensure Driver implements the required interfaces
var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)
