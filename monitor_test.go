// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/luxfi/log"
)

func TestMonitorDoubleClose(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))
	require.True(t, mon.shouldRun())
	mon.Close()
	require.False(t, mon.shouldRun())
	mon.Close()
	require.False(t, mon.shouldRun())
}

func TestMonitorPrematureCancelTask(t *testing.T) {
	start := time.Now()

	ticked := make(chan struct{})

	logger := makeLogger(t)
	mon := NewMonitor(start, logger)
	logger.intercept(func(msg string) {
		if msg == "Ticked" {
			ticked <- struct{}{}
		}
	})

	t.Run("Cancelled future task does not fire", func(t *testing.T) {
		panicFn := func() {
			panic("test failed")
		}

		mon.FutureTask(time.Hour, panicFn)
		mon.CancelFutureTask()

		mon.AdvanceTime(start.Add(time.Hour))

		select {
		case <-ticked:
		case <-time.After(time.Minute):
			require.FailNow(t, "timed out waiting on tick")
		}
	})

	t.Run("Non-Cancelled future task fires", func(t *testing.T) {
		finish := make(chan struct{})

		mon.FutureTask(time.Hour, func() {
			close(finish)
		})

		mon.AdvanceTime(start.Add(time.Hour * 2))

		<-ticked
		<-finish
	})

	t.Run("Cancelled task does not fire", func(t *testing.T) {
		finish := make(chan struct{})

		mon.RunTask(func() {
			<-finish
			close(finish) // Test should panic if we have a double close
		})

		mon.CancelTask()

		close(finish)
	})
}

func TestMonitorAsyncWaitFor(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.RunTask(wg.Done)
	wg.Wait()
}

func TestMonitorAsyncWaitUntilWithWaitFor(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.FutureTask(10*time.Millisecond, wg.Done)
	mon.RunTask(func() {
		mon.AdvanceTime(start.Add(10 * time.Millisecond))
	})
	wg.Wait()
}

func TestMonitorAsyncWaitForWithNestedWaitUntil(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.RunTask(func() {
		go mon.AdvanceTime(start.Add(10 * time.Millisecond))
		mon.FutureTask(10*time.Millisecond, wg.Done)
	})
	wg.Wait()
}

// testLogger wraps log.Logger with intercept capability for testing
type testLogger struct {
	log.Logger
	hook func(msg string)
}

func (tl *testLogger) intercept(hook func(msg string)) {
	tl.hook = hook
}

func (tl *testLogger) Trace(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Trace(msg, ctx...)
}

func (tl *testLogger) Verbo(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Debug(msg, ctx...)
}

func (tl *testLogger) Debug(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Debug(msg, ctx...)
}

func (tl *testLogger) Info(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Info(msg, ctx...)
}

func (tl *testLogger) Warn(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Warn(msg, ctx...)
}

func (tl *testLogger) Error(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Error(msg, ctx...)
}

func (tl *testLogger) Fatal(msg string, fields ...log.Field) {
	if tl.hook != nil {
		tl.hook(msg)
	}
	ctx := make([]interface{}, len(fields))
	for i, f := range fields {
		ctx[i] = f
	}
	tl.Logger.Fatal(msg, ctx...)
}

func makeLogger(t *testing.T) *testLogger {
	w := &testWriter{t: t}
	logger := log.NewWriter(w).With().Str("test", t.Name()).Logger()
	return &testLogger{Logger: logger}
}

type testWriter struct {
	t *testing.T
}

func (tw *testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}

// Ensure testWriter implements io.Writer
var _ io.Writer = (*testWriter)(nil)
