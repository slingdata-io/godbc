//go:build windows

package odbc

import (
	"syscall"
)

// loadODBCLibrary loads the ODBC library on Windows
func loadODBCLibrary(libPath string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(libPath)
	if err != nil {
		return 0, err
	}
	return uintptr(handle), nil
}
