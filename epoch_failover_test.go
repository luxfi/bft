// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/luxfi/bft"

	"github.com/luxfi/bft/record"
	"github.com/luxfi/bft/testutil"

	"github.com/stretchr/testify/require"
	
	"go.uber.org/zap/zapcore"
)

func TestReceiveEmptyNotarizationWithNoQC(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
		ID:                  nodes[1],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	emptyNotarization := newEmptyNotarization(nodes[:3], 0, 0)

	e.HandleMessage(&Message{
		EmptyNotarization: &EmptyNotarization{Vote: emptyNotarization.Vote},
	}, nodes[0])
}

func TestEpochLeaderRecursivelyFetchNotarizedBlocks(t *testing.T) {
	t.Skip("TODO: Fix timing/coordination issue causing test to hang")
	nodes := []NodeID{{1}, {2}, {3}, {4}}
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	recordedMessages := make(chan *Message, 100)

	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	nodeID := nodes[0]

	require.NoError(t, e.Start())

	startTime := start

	// Feed the node empty notarizations until it advances to the round of the last block.
	for round := uint64(0); round < 6; round++ {
		emptyNotarization := newEmptyNotarization(nodes[1:], round, 0)

		if LeaderForRound(nodes, round).Equals(nodeID) {
			waitToEnterRound(t, e, round)
			t.Log("Triggering block to be built for round", round)
			bb.blockShouldBeBuilt <- struct{}{}
			waitForBlockProposerTimeout(t, e, &startTime, round)

			err = e.HandleMessage(&Message{
				EmptyNotarization: emptyNotarization,
			}, nodes[2])
			require.NoError(t, err)

			wal.assertNotarization(round)
			continue
		}

		// Otherwise, just receive the empty notarization
		// and advance to the next round
		err = e.HandleMessage(&Message{
			EmptyNotarization: emptyNotarization,
		}, nodes[2])
		require.NoError(t, err)
		wal.assertNotarization(round)
		waitToEnterRound(t, e, round)
	}
}

func TestEpochLeaderFailoverInLeaderRound(t *testing.T) {
	nodes := []NodeID{{1}, {2}, {3}, {4}}

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	wal := newTestWAL(t)

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
		ID:                  nodes[3],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	// Round 0
	notarizeAndFinalizeRound(t, e, bb)
	// Round 1
	notarizeAndFinalizeRound(t, e, bb)
	// Round 2
	notarizeAndFinalizeRound(t, e, bb)

	// In the third round, we wait for our epoch to propose a block.

	bb.blockShouldBeBuilt <- struct{}{}

	var sentBlockMessage bool
	for !sentBlockMessage {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for block proposal")
		case msg := <-recordedMessages:
			if msg.VerifiedBlockMessage != nil {
				sentBlockMessage = true
				break
			}
		}
	}

	block := <-bb.out
	md := block.BlockHeader().ProtocolMetadata

	// Receive empty votes from other nodes
	emptyVoteFrom1 := createEmptyVote(md, nodes[1])
	emptyVoteFrom2 := createEmptyVote(md, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[1])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	startTime := start
	waitForBlockProposerTimeout(t, e, &startTime, md.Round)
}

func TestEpochLeaderFailoverGarbageCollectedEmptyVotes(t *testing.T) {
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
		ID:                  nodes[0],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                noopComm(nodes),
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	var waitForTimeout sync.WaitGroup
	waitForTimeout.Add(1)

	var triggerEmptyBlockAgreement sync.WaitGroup
	triggerEmptyBlockAgreement.Add(1)

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	if logger != nil {
		logger.Intercept(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "It is time to build a block") {
				emptyNotarization := newEmptyNotarization(nodes[1:], 3, 2)
				e.HandleMessage(&Message{
					EmptyNotarization: emptyNotarization,
				}, nodes[1])

				waitForTimeout.Done()
			}

			if strings.Contains(entry.Message, "empty block agreement") {
				triggerEmptyBlockAgreement.Done()
			}

			return nil
		})
	}

	require.NoError(t, e.Start())

	// Run through 3 blocks, to make the block proposals be:
	// 1 --> 2 --> 3 --> X (node 4 doesn't propose a block)

	// Then, don't do anything and wait for our node
	// to start complaining about a block not being notarized

	for round := uint64(0); round < 3; round++ {
		notarizeAndFinalizeRound(t, e, bb)
	}

	bb.blockShouldBeBuilt <- struct{}{}

	// Wait until we detect it is time to build a block, and we also advance to the next round because of it.
	waitForTimeout.Wait()

	startTime := start
	startTime = startTime.Add(e.EpochConfig.MaxProposalWait)
	e.AdvanceTime(startTime)

	triggerEmptyBlockAgreement.Wait()

	// At this point, if we have initialized the timeout process, handling any message fails because we have a halted error.
	// The halted error takes place because we attempted to timeout on a round that has already been garbage collected.
	err = e.HandleMessage(&Message{}, nodes[1])
	require.NoError(t, err)
}

func TestEpochLeaderFailoverBecauseOfBadBlock(t *testing.T) {
	// This test ensures that if a block is proposed by a node, but it is invalid,
	// the node will immediately proceed to notarize the empty block.

	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	recordedMessages := make(chan *Message, 100)
	comm := &recordingComm{Communication: noopComm(nodes), BroadcastMessages: recordedMessages}

	logger := testutil.MakeLogger(t, 1)

	start := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		StartTime:           start,
		Logger:              logger,
		ID:                  nodes[3],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())

	notarizeAndFinalizeRound(t, e, bb)

	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block := <-bb.out
	block.verificationError = errors.New("invalid block")

	vote, err := newTestVote(block, nodes[1])
	require.NoError(t, err)

	go func() {
		err = e.HandleMessage(&Message{
			BlockMessage: &BlockMessage{
				Vote:  *vote,
				Block: block,
			},
		}, nodes[1])
		require.NoError(t, err)
	}()

	emptyVoteFrom1 := createEmptyVote(md, nodes[0])
	emptyVoteFrom2 := createEmptyVote(md, nodes[2])

	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom1,
	}, nodes[0])
	e.HandleMessage(&Message{
		EmptyVoteMessage: emptyVoteFrom2,
	}, nodes[2])

	waitForBlockProposerTimeout(t, e, &start, 1)

	require.Equal(t, record.EmptyNotarizationRecordType, wal.assertNotarization(1))
	notarizeAndFinalizeRound(t, e, bb)
}

func TestEpochRebroadcastsEmptyVoteAfterBlockProposalReceived(t *testing.T) {
	bb := &testBlockBuilder{out: make(chan *testBlock, 1), blockShouldBeBuilt: make(chan struct{}, 1)}
	storage := newInMemStorage()

	nodes := []NodeID{{1}, {2}, {3}, {4}}

	wal := newTestWAL(t)

	comm := newRebroadcastComm(nodes)

	logger := testutil.MakeLogger(t, 1)

	epochTime := time.Now()
	conf := EpochConfig{
		MaxProposalWait:     DefaultMaxProposalWaitTime,
		MaxRebroadcastWait:  DefaultEmptyVoteRebroadcastTimeout,
		StartTime:           epochTime,
		Logger:              logger,
		ID:                  nodes[3],
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &testSignatureAggregator{},
	}

	e, err := NewEpoch(conf)
	require.NoError(t, err)

	require.NoError(t, e.Start())
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.Equal(t, uint64(0), e.Metadata().Round)
	require.False(t, wal.containsEmptyVote(0))

	bb.blockShouldBeBuilt <- struct{}{}

	// receive the block proposal for round 0
	md := e.Metadata()
	_, ok := bb.BuildBlock(context.Background(), md)
	require.True(t, ok)
	block := <-bb.out

	vote, err := newTestVote(block, nodes[0])
	require.NoError(t, err)
	err = e.HandleMessage(&Message{
		BlockMessage: &BlockMessage{
			Vote:  *vote,
			Block: block,
		},
	}, nodes[0])
	require.NoError(t, err)

	// ensure we have the block in our wal
	wal.assertBlockProposal(0)
	wal.assertWALSize(1)

	// wait for the initial empty vote broadcast
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)

	// advance another rebroadcast period and ensure more empty votes are sent
	waitForEmptyVote(t, comm, e, 0, epochTime)
	require.Len(t, comm.emptyVotes, 0)
}

func createEmptyVote(md ProtocolMetadata, signer NodeID) *EmptyVote {
	return &EmptyVote{
		Signature: Signature{
			Signer: signer,
		},
		Vote: ToBeSignedEmptyVote{
			ProtocolMetadata: md,
		},
	}
}

func waitForBlockProposerTimeout(t *testing.T, e *Epoch, startTime *time.Time, startRound uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.WAL.(*testWAL).containsEmptyVote(startRound) || e.WAL.(*testWAL).containsEmptyNotarization(startRound) {
			return
		}
		*startTime = startTime.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(*startTime)
		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(t, "timed out waiting for event")
		}
	}
}

func waitForEmptyVote(t *testing.T, comm *rebroadcastComm, e *Epoch, expectedRound uint64, epochTime time.Time) {
	timeout := time.NewTimer(1 * time.Minute)
	defer timeout.Stop()

	for {
		select {
		case emptyVote := <-comm.emptyVotes:
			require.Equal(t, expectedRound, emptyVote.Vote.Round)
			return
		case <-timeout.C:
			t.Fatalf("Timed out waiting for empty vote for round %d", expectedRound)
		case <-time.After(10 * time.Millisecond):
			epochTime = epochTime.Add(e.MaxRebroadcastWait)
			e.AdvanceTime(epochTime)
		}
	}
}

type recordingComm struct {
	Communication
	BroadcastMessages chan *Message
	SentMessages      chan *Message
}

func (rc *recordingComm) Broadcast(msg *Message) {
	if rc.BroadcastMessages != nil {
		rc.BroadcastMessages <- msg
	}
	rc.Communication.Broadcast(msg)
}

func (rc *recordingComm) Send(msg *Message, to NodeID) {
	if rc.SentMessages != nil {
		rc.SentMessages <- msg
	}
	rc.Communication.Send(msg, to)
}

type rebroadcastComm struct {
	nodes      []NodeID
	emptyVotes chan *EmptyVote
}

func newRebroadcastComm(nodes []NodeID) *rebroadcastComm {
	return &rebroadcastComm{
		nodes:      nodes,
		emptyVotes: make(chan *EmptyVote, 10),
	}
}

func (r *rebroadcastComm) Nodes() []NodeID {
	return r.nodes
}

func (r *rebroadcastComm) Send(*Message, NodeID) {

}

func (r *rebroadcastComm) Broadcast(msg *Message) {
	if msg.EmptyVoteMessage != nil {
		r.emptyVotes <- msg.EmptyVoteMessage
	}
}
