// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

	mon := NewMonitor(start, makeLogger(t))
	mon.logger.(*testLogger).intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Ticked" {
			ticked <- struct{}{}
		}
		return nil
	})

	t.Run("Cancelled future task does not fire", func(t *testing.T) {
		panic := func() {
			panic("test failed")
		}

		mon.FutureTask(time.Hour, panic)
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

type testLogger struct {
	*zap.Logger
}

func (tl *testLogger) Trace(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *testLogger) Verbo(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

func (t *testLogger) intercept(hook func(entry zapcore.Entry) error) {
	logger := t.Logger.WithOptions(zap.Hooks(hook))
	t.Logger = logger
}

func makeLogger(t *testing.T) *testLogger {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	return &testLogger{Logger: logger}
}
