# TestReplicationNodeDiverges flake — repro + root cause

**Symptom**: 1/10 to 1/20 runs of `TestReplicationNodeDiverges` (`replication_test.go:596`)
hang to 60s timeout on darwin/arm64. Reported by scientist 2026-05-13.

## Repro

```bash
cd ~/work/lux/bft
go test -run TestReplicationNodeDiverges -count=10 -timeout=180s -v ./...
```

Typical failure mode (one observed run, full stack):

```
github.com/luxfi/bft/testutil.(*TestLogger).Info(...)
	/Users/z/work/lux/bft/testutil/logger.go:89 +0x12c
github.com/luxfi/bft.(*Epoch).indexFinalization(...)
	/Users/z/work/lux/bft/epoch.go:1074 +0x194
github.com/luxfi/bft.(*Epoch).processFinalizedBlock.(*Epoch).createFinalizedBlockVerificationTask.func1()
	/Users/z/work/lux/bft/epoch.go:1750 +0x428
github.com/luxfi/bft.(*scheduler).run(0x...)
	/Users/z/work/lux/bft/sched.go:107 +0x25c
created by github.com/luxfi/bft.NewScheduler in goroutine 20
	/Users/z/work/lux/bft/sched.go:27 +0x104
```

## Root cause

`scheduler.Close()` (`sched.go:39`) sets `as.close = true` and broadcasts the
signal — but does NOT wait for the goroutine started at `sched.go:27` (`go as.run()`)
to exit. If a task is mid-execution (`taskToRun.f()` at `sched.go:107`) when
`Close()` returns, the goroutine continues for one more iteration, calls
`indexFinalization` → `t.Log` via the test logger.

By that point the test function has often returned. `t.Log` from a goroutine
after the test has finished is a documented data race in Go's testing package
— it panics or returns garbage, manifesting as a hang on the next allocation
because the test infrastructure has already released its state.

This is precisely what upstream `ava-labs/Simplex` PR #374 ("Detect goroutine
leaks in tests") was added to catch — and what their `leak_test.go` file
prevents at fork time. We don't have either.

## Fix (proposed — separate PR)

```go
type scheduler struct {
    // ... existing fields ...
    done chan struct{}      // NEW
}

func NewScheduler(logger Logger) *scheduler {
    var as scheduler
    as.pending = newDependencies()
    as.signal = sync.Cond{L: &as.lock}
    as.logger = logger
    as.done = make(chan struct{})     // NEW

    go func() {
        defer close(as.done)           // NEW
        as.run()
    }()

    return &as
}

func (as *scheduler) Close() {
    as.lock.Lock()
    as.close = true
    as.signal.Broadcast()
    as.lock.Unlock()
    <-as.done                          // NEW — wait for goroutine exit
}
```

Plus port `leak_test.go` from upstream Simplex (see task #56) so any future
goroutine leak fails the test loudly instead of silently flaking.

## Mitigation landed (partial)

`epoch_multinode_test.go:newSimplexNode` now registers a `t.Cleanup` that
calls `ti.l.Silence()` — turns the test logger into a no-op before the
*testing.T is torn down. This eliminates the `Log in goroutine after
Test... has completed` panic flake for the SCHEDULER goroutine.

It does NOT eliminate every flake. The remaining failure mode is
`handleMessages` (line 229-237) calling `require.NoError(t.t, err)`
after the test has ended — `require` uses `t.Errorf` which panics
post-test. Fixing that requires either:
1. Goroutine-join cleanup (deadlock-prone — earlier attempt timed out
   at 300s because tasks block on locks the test no longer services).
2. Replacing `require.NoError` with a `recover()`-guarded check or a
   sentinel-channel-based exit.
3. Porting upstream Simplex `leak_test.go` + scheduler refactor (PR
   #296) wholesale — task #56.

Recommendation: do (3). The silence-cleanup buys us most of the
robustness today; the upstream sync gives us the proper lifecycle.

## Related

- Task #53 (this) — repro + root cause + partial mitigation
- Task #56 — sync simplex upstream (gives us the leak detector + the
  upstream-side scheduler refactor PR #296)
- Scientist's verification at 1/20 reproduction rate on darwin/arm64
- This file lives next to the test; do NOT add it to release notes — it's
  internal engineering debt tracking.
