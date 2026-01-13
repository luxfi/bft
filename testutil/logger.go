// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"io"
	"os"
	"testing"

	"github.com/luxfi/log"
)

// LogEntry represents a captured log entry for testing
type LogEntry struct {
	Level   log.Level
	Message string
}

// TestLogger wraps log.Logger with test utilities
type TestLogger struct {
	log.Logger
	silenced bool
	hook     func(entry LogEntry)
}

// Silence stops all logging output
func (t *TestLogger) Silence() {
	t.silenced = true
	t.Logger = log.Noop()
}

// Intercept sets a hook function that receives all log entries
func (tl *TestLogger) Intercept(hook func(entry LogEntry)) {
	tl.hook = hook
}

func (tl *TestLogger) runHook(level log.Level, msg string) {
	if tl.hook != nil {
		tl.hook(LogEntry{Level: level, Message: msg})
	}
}

func (tl *TestLogger) Trace(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.TraceLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Trace(msg, ctx...)
}

func (tl *TestLogger) Verbo(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.DebugLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Debug(msg, ctx...)
}

func (tl *TestLogger) Debug(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.DebugLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Debug(msg, ctx...)
}

func (tl *TestLogger) Info(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.InfoLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Info(msg, ctx...)
}

func (tl *TestLogger) Warn(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.WarnLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Warn(msg, ctx...)
}

func (tl *TestLogger) Error(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.ErrorLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Error(msg, ctx...)
}

func (tl *TestLogger) Fatal(msg string, fields ...log.Field) {
	if tl.silenced {
		return
	}
	tl.runHook(log.FatalLevel, msg)
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Fatal(msg, ctx...)
}

// MakeLogger creates a TestLogger for testing
func MakeLogger(t *testing.T, node ...int) *TestLogger {
	var w io.Writer = os.Stdout
	if t != nil {
		w = &testWriter{t: t}
	}

	logger := log.NewWriter(w).With().
		Str("test", t.Name()).
		Logger()

	if len(node) > 0 {
		logger = logger.With().Int("node", node[0]).Logger()
	}

	return &TestLogger{Logger: logger}
}

// testWriter wraps testing.T for log output
type testWriter struct {
	t *testing.T
}

func (tw *testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}
