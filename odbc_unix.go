//go:build !windows

package godbc

import (
	"github.com/ebitengine/purego"
)

// loadODBCLibrary loads the ODBC library on Unix-like systems
func loadODBCLibrary(libPath string) (uintptr, error) {
	return purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
}
