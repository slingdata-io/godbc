package godbc

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
)

// FFI tracing.
//
// Set GODBC_DEBUG=1 to log every ODBC FFI call *before* control enters the C
// driver, including its arguments. There is no matching post-call log line: if
// the driver aborts the process (e.g. "*** stack smashing detected ***" /
// SIGABRT) the LAST logged call is the one that crashed, and its arguments
// are right there in the line.
//
// Records go to stderr via log/slog and are fsync'd immediately, so the final
// line survives a C-side abort() that never returns to Go.
//
// This is zero-overhead unless GODBC_DEBUG=1 (a single sync.Once-guarded
// pointer load per call when disabled).

// Logger traces ODBC FFI calls. The zero value is unusable; use the package
// logger instance, which lazily initializes from GODBC_DEBUG.
type Logger struct {
	once sync.Once
	slog *slog.Logger // nil unless GODBC_DEBUG=1
}

// logger is the package-wide FFI tracer used by the ODBC wrappers.
var logger = &Logger{}

// syncHandler wraps a slog.Handler and fsyncs stderr after every record so the
// last line is durable before a potential abort() inside the C call.
type syncHandler struct{ slog.Handler }

func (h syncHandler) Handle(ctx context.Context, r slog.Record) error {
	err := h.Handler.Handle(ctx, r)
	_ = os.Stderr.Sync()
	return err
}

func (l *Logger) init() {
	l.once.Do(func() {
		if os.Getenv("GODBC_DEBUG") != "1" {
			return
		}
		base := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
		l.slog = slog.New(syncHandler{base})
		l.slog.Debug("FFI tracing enabled", "pid", os.Getpid())
	})
}

// Debug logs an FFI call about to be made, with its arguments. It is a no-op
// unless GODBC_DEBUG=1. Call it immediately before the native call:
//
//	logger.Debug("SQLExecDirect", "stmt", fmt.Sprintf("%#x", h), "textLen", n)
func (l *Logger) Debug(name string, args ...any) {
	l.init()
	if l.slog == nil {
		return
	}
	l.slog.Debug("ffi call", append([]any{"fn", name}, args...)...)
}

// handleTypeName names an ODBC handle-type constant for readable trace lines.
func handleTypeName(t SQLSMALLINT) string {
	switch t {
	case SQL_HANDLE_ENV:
		return "ENV"
	case SQL_HANDLE_DBC:
		return "DBC"
	case SQL_HANDLE_STMT:
		return "STMT"
	case SQL_HANDLE_DESC:
		return "DESC"
	default:
		return "?"
	}
}

// debugHex returns an offset/hex/ASCII dump (capped) of b as a single
// slog-attribute value, for logging the exact bytes handed to the driver.
func debugHex(b []byte) string {
	const max = 256
	var sb strings.Builder
	n := len(b)
	if b == nil {
		return "<nil>"
	}
	fmt.Fprintf(&sb, "len=%d", n)
	if n > 0 {
		fmt.Fprintf(&sb, " ptr=%#x", &b[0])
	}
	if cap(b) != n {
		fmt.Fprintf(&sb, " cap=%d", cap(b))
	}
	shown := n
	if shown > max {
		shown = max
	}
	for off := 0; off < shown; off += 16 {
		end := off + 16
		if end > shown {
			end = shown
		}
		fmt.Fprintf(&sb, " | %08x ", off)
		for i := off; i < end; i++ {
			fmt.Fprintf(&sb, "%02x ", b[i])
		}
		sb.WriteByte('"')
		for i := off; i < end; i++ {
			c := b[i]
			if c < 0x20 || c > 0x7e {
				c = '.'
			}
			sb.WriteByte(c)
		}
		sb.WriteByte('"')
	}
	if n > shown {
		fmt.Fprintf(&sb, " ...(%d more bytes)", n-shown)
	}
	return sb.String()
}

var debugPwdRe = regexp.MustCompile(`(?i)\b(PWD|PASSWORD)=([^;]*)`)

// debugMaskConnStr redacts the password in an ODBC connection string so it is
// safe to include in shareable debug output.
func debugMaskConnStr(s string) string {
	return debugPwdRe.ReplaceAllString(s, "$1=***")
}
