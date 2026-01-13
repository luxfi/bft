// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSilence(t *testing.T) {
	l1 := MakeLogger(t)
	l2 := MakeLogger(t)

	l1.Silence()

	l1.Intercept(func(entry LogEntry) {
		t.Fatal("shouldn't be logged")
	})

	var c int

	l2.Intercept(func(entry LogEntry) {
		c++
	})

	l1.Debug("Debug message")
	l1.Info("Info message")

	l2.Debug("Debug message")
	l2.Info("Info message")

	require.Equal(t, 2, c)
}
