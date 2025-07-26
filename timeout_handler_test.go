// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/bft"
	"github.com/luxfi/bft/testutil"

	"github.com/stretchr/testify/require"
)

func TestAddAndRunTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []bft.NodeID{{1}, {2}}
	handler := bft.NewTimeoutHandler(l, start, nodes)
	defer handler.Close()

	sent := make(chan struct{}, 1)
	var count atomic.Int64

	task := &bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "simplerun",
		Deadline: start.Add(5 * time.Second),
		Task: func() {
			sent <- struct{}{}
			count.Add(1)
		},
	}

	handler.AddTask(task)
	handler.Tick(start.Add(2 * time.Second))
	time.Sleep(10 * time.Millisecond)

	require.Zero(t, len(sent))
	handler.Tick(start.Add(6 * time.Second))
	<-sent
	require.Equal(t, int64(1), count.Load())

	// test we only execute task once
	handler.Tick(start.Add(12 * time.Second))
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, int64(1), count.Load())
}

func TestRemoveTask(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []bft.NodeID{{1}, {2}}
	handler := bft.NewTimeoutHandler(l, start, nodes)
	defer handler.Close()

	var ran bool
	task := &bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "task2",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			ran = true
		},
	}

	handler.AddTask(task)
	handler.RemoveTask(nodes[0], "task2")
	handler.Tick(start.Add(2 * time.Second))
	require.False(t, ran)

	// ensure no panic
	handler.RemoveTask(nodes[1], "task-doesn't-exist")
}

func TestTaskOrder(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []bft.NodeID{{1}, {2}}
	handler := bft.NewTimeoutHandler(l, start, nodes)
	defer handler.Close()

	finished := make(chan struct{})

	var mu sync.Mutex
	var results []string

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "first",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[1],
		TaskID:   "second",
		Deadline: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "noruntask",
		Deadline: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "norun")
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(3 * time.Second))

	<-finished
	<-finished

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 2, len(results))
	require.Equal(t, results[0], "first")
	require.Equal(t, results[1], "second")
}

func TestAddTasksOutOfOrder(t *testing.T) {
	start := time.Now()
	l := testutil.MakeLogger(t, 1)
	nodes := []bft.NodeID{{1}, {2}}
	handler := bft.NewTimeoutHandler(l, start, nodes)
	defer handler.Close()

	finished := make(chan struct{})
	var mu sync.Mutex
	var results []string

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "third",
		Deadline: start.Add(3 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "third")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "second",
		Deadline: start.Add(2 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "second")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[1],
		TaskID:   "fourth",
		Deadline: start.Add(4 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "fourth")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.AddTask(&bft.TimeoutTask{
		NodeID:   nodes[0],
		TaskID:   "first",
		Deadline: start.Add(1 * time.Second),
		Task: func() {
			mu.Lock()
			results = append(results, "first")
			finished <- struct{}{}
			mu.Unlock()
		},
	})

	handler.Tick(start.Add(1 * time.Second))
	<-finished
	mu.Lock()
	require.Equal(t, 1, len(results))
	require.Equal(t, results[0], "first")
	mu.Unlock()

	handler.Tick(start.Add(3 * time.Second))
	<-finished
	<-finished
	mu.Lock()
	require.Equal(t, 3, len(results))
	require.Equal(t, results[1], "second")
	require.Equal(t, results[2], "third")
	mu.Unlock()

	handler.Tick(start.Add(4 * time.Second))
	<-finished
	mu.Lock()
	require.Equal(t, 4, len(results))
	require.Equal(t, results[3], "fourth")
	mu.Unlock()
}

func TestFindTask(t *testing.T) {
	// Setup a mock logger
	l := testutil.MakeLogger(t, 1)
	nodes := []bft.NodeID{{1}, {2}}
	startTime := time.Now()

	handler := bft.NewTimeoutHandler(l, startTime, nodes)
	defer handler.Close()

	// Create some test tasks
	task1 := &bft.TimeoutTask{
		TaskID: "task1",
		NodeID: nodes[0],
		Start:  5,
		End:    10,
	}

	taskSameRangeDiffNode := &bft.TimeoutTask{
		TaskID: "taskSameDiff",
		NodeID: nodes[1],
		Start:  5,
		End:    10,
	}

	task3 := &bft.TimeoutTask{
		TaskID: "task3",
		NodeID: nodes[1],
		Start:  25,
		End:    30,
	}

	task4 := &bft.TimeoutTask{
		TaskID: "task4",
		NodeID: nodes[1],
		Start:  31,
		End:    36,
	}

	// Add tasks to handler
	handler.AddTask(task1)
	handler.AddTask(taskSameRangeDiffNode)
	handler.AddTask(task3)
	handler.AddTask(task4)

	tests := []struct {
		name     string
		node     bft.NodeID
		seqs     []uint64
		expected *bft.TimeoutTask
	}{
		{
			name:     "Find task with sequence in middle of range",
			node:     nodes[0],
			seqs:     []uint64{7, 8, 9},
			expected: task1,
		},
		{
			name:     "Find task with sequence at boundary (inclusive)",
			node:     nodes[0],
			seqs:     []uint64{5, 7},
			expected: task1,
		},
		{
			name:     "Find task with mixed sequences (first valid sequence)",
			node:     nodes[0],
			seqs:     []uint64{3, 4, 5, 11},
			expected: task1, // 5 is in range
		},
		{
			name:     "Same sequences, but different node",
			node:     nodes[1],
			seqs:     []uint64{7, 8, 9},
			expected: taskSameRangeDiffNode,
		},
		{
			name:     "No sequences in range",
			node:     nodes[0],
			seqs:     []uint64{1, 2, 3, 4, 11, 12, 13, 14},
			expected: nil,
		},
		{
			name:     "Span across many tasks",
			node:     nodes[1],
			seqs:     []uint64{26, 27, 30, 31, 33},
			expected: task3,
		},
		{
			name:     "Unknown node",
			node:     bft.NodeID("unknown"),
			seqs:     []uint64{5, 15, 25},
			expected: nil,
		},
		{
			name:     "Empty sequence list",
			node:     nodes[1],
			seqs:     []uint64{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bft.FindReplicationTask(handler, tt.node, tt.seqs)
			if tt.expected != result {
				require.Fail(t, "not equal")
			}
			require.Equal(t, tt.expected, result)
		})
	}
}
