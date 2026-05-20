//go:build leak

// Copyright (C) 2019-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every bft test and then verifies no goroutines leaked
// after each test returned. Without this, post-test logging from
// orphaned scheduler / messageHandler / replication goroutines races
// against testing.T teardown and surfaces as the 5–10% flake repro'd
// in REPLICATION_FLAKE_REPRO.md.
//
// Build-tag gated: `go test -tags leak ./...` runs with detection;
// default `go test` skips this file. Once the scheduler / replication
// lifecycle refactor lands (porting upstream Simplex PR #296 + #303),
// the build tag can be removed and detection runs unconditionally.
//
// Ported from upstream Simplex PR #374 (2026-04-29) — see task #56.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
