package bft_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/luxfi/bft"

	"github.com/stretchr/testify/require"
)

// TestReplicationRequestIndexedBlocks tests replication requests for indexed blocks.
func TestReplicationRequestIndexedBlocks(t *testing.T) {
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []bft.NodeID{{1}, {2}, {3}, {4}}
	comm := NewListenerComm(nodes)
	ctx := context.Background()
	conf := defaultTestNodeEpochConfig(t, nodes[0], comm, bb)
	conf.ReplicationEnabled = true

	numBlocks := uint64(10)
	seqs := createBlocks(t, nodes, bb, numBlocks)
	for _, data := range seqs {
		err := conf.Storage.Index(ctx, data.VerifiedBlock, data.Finalization)
		require.NoError(t, err)
	}
	e, err := bft.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())
	sequences := []uint64{0, 1, 2, 3}
	req := &bft.Message{
		ReplicationRequest: &bft.ReplicationRequest{
			Seqs:        sequences,
			LatestRound: numBlocks,
		},
	}

	err = e.HandleMessage(req, nodes[1])
	require.NoError(t, err)

	msg := <-comm.in
	resp := msg.VerifiedReplicationResponse
	require.Nil(t, resp.LatestRound)

	require.Equal(t, len(sequences), len(resp.Data))
	for i, data := range resp.Data {
		require.Equal(t, seqs[i].Finalization, *data.Finalization)
		require.Equal(t, seqs[i].VerifiedBlock, data.VerifiedBlock)
	}

	// request out of scope
	req = &bft.Message{
		ReplicationRequest: &bft.ReplicationRequest{
			Seqs: []uint64{11, 12, 13},
		},
	}

	err = e.HandleMessage(req, nodes[1])
	require.NoError(t, err)

	msg = <-comm.in
	resp = msg.VerifiedReplicationResponse
	require.Zero(t, len(resp.Data))
}

// TestReplicationRequestNotarizations tests replication requests for notarized blocks.
func TestReplicationRequestNotarizations(t *testing.T) {
	// generate 5 blocks & notarizations
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []bft.NodeID{{1}, {2}, {3}, {4}}
	comm := NewListenerComm(nodes)
	conf := defaultTestNodeEpochConfig(t, nodes[0], comm, bb)
	conf.ReplicationEnabled = true

	e, err := bft.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	numBlocks := uint64(5)
	rounds := make(map[uint64]bft.VerifiedQuorumRound)
	for i := uint64(0); i < numBlocks; i++ {
		block, notarization := advanceRoundFromNotarization(t, e, bb)

		rounds[i] = bft.VerifiedQuorumRound{
			VerifiedBlock: block,
			Notarization:  notarization,
		}
	}

	require.Equal(t, uint64(numBlocks), e.Metadata().Round)

	seqs := make([]uint64, 0, len(rounds))
	for k := range rounds {
		seqs = append(seqs, k)
	}
	req := &bft.Message{
		ReplicationRequest: &bft.ReplicationRequest{
			Seqs:        seqs,
			LatestRound: 0,
		},
	}

	err = e.HandleMessage(req, nodes[1])
	require.NoError(t, err)

	msg := <-comm.in
	resp := msg.VerifiedReplicationResponse
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, *resp.LatestRound, rounds[numBlocks-1])
	for _, round := range resp.Data {
		require.Nil(t, round.EmptyNotarization)
		notarizedBlock, ok := rounds[round.VerifiedBlock.BlockHeader().Round]
		require.True(t, ok)
		require.Equal(t, notarizedBlock.VerifiedBlock, round.VerifiedBlock)
		require.Equal(t, notarizedBlock.Notarization, round.Notarization)
	}
}

// TestReplicationRequestMixed ensures the replication response also includes empty notarizations
func TestReplicationRequestMixed(t *testing.T) {
	// generate 5 blocks & notarizations
	bb := &testBlockBuilder{out: make(chan *testBlock, 1)}
	nodes := []bft.NodeID{{1}, {2}, {3}, {4}}
	comm := NewListenerComm(nodes)
	conf := defaultTestNodeEpochConfig(t, nodes[0], comm, bb)
	conf.ReplicationEnabled = true

	e, err := bft.NewEpoch(conf)
	require.NoError(t, err)
	require.NoError(t, e.Start())

	numBlocks := uint64(8)
	rounds := make(map[uint64]bft.VerifiedQuorumRound)
	// only produce a notarization for blocks we are the leader, otherwise produce an empty notarization
	for i := range numBlocks {
		leaderForRound := bytes.Equal(bft.LeaderForRound(nodes, uint64(i)), e.ID)
		emptyBlock := !leaderForRound
		if emptyBlock {
			emptyNotarization := newEmptyNotarization(nodes, uint64(i), uint64(i))
			e.HandleMessage(&bft.Message{
				EmptyNotarization: emptyNotarization,
			}, nodes[1])
			e.WAL.(*testWAL).assertNotarization(uint64(i))
			rounds[i] = bft.VerifiedQuorumRound{
				EmptyNotarization: emptyNotarization,
			}
			continue
		}
		block, notarization := advanceRoundFromNotarization(t, e, bb)

		rounds[i] = bft.VerifiedQuorumRound{
			VerifiedBlock: block,
			Notarization:  notarization,
		}
	}

	require.Equal(t, uint64(numBlocks), e.Metadata().Round)
	seqs := make([]uint64, 0, len(rounds))
	for k := range rounds {
		seqs = append(seqs, k)
	}

	req := &bft.Message{
		ReplicationRequest: &bft.ReplicationRequest{
			Seqs:        seqs,
			LatestRound: 0,
		},
	}

	err = e.HandleMessage(req, nodes[1])
	require.NoError(t, err)

	msg := <-comm.in
	resp := msg.VerifiedReplicationResponse

	require.Equal(t, *resp.LatestRound, rounds[numBlocks-1])
	for _, round := range resp.Data {
		notarizedBlock, ok := rounds[round.GetRound()]
		require.True(t, ok)
		require.Equal(t, notarizedBlock.VerifiedBlock, round.VerifiedBlock)
		require.Equal(t, notarizedBlock.Notarization, round.Notarization)
		require.Equal(t, notarizedBlock.EmptyNotarization, round.EmptyNotarization)
	}
}

func TestNilReplicationResponse(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []bft.NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)

	normalNode0 := newBFTNode(t, nodes[0], net, bb, nil)
	normalNode0.start()

	err := normalNode0.HandleMessage(&bft.Message{
		ReplicationResponse: &bft.ReplicationResponse{
			Data: []bft.QuorumRound{{}},
		},
	}, nodes[1])
	require.NoError(t, err)
}

// TestMalformedReplicationResponse tests that a malformed replication response is handled correctly.
// This replication response is malformeds since it must also include a notarization or
// finalization.
func TestMalformedReplicationResponse(t *testing.T) {
	bb := newTestControlledBlockBuilder(t)
	nodes := []bft.NodeID{{1}, {2}, {3}, {4}}
	net := newInMemNetwork(t, nodes)

	normalNode0 := newBFTNode(t, nodes[0], net, bb, nil)
	normalNode0.start()

	err := normalNode0.HandleMessage(&bft.Message{
		ReplicationResponse: &bft.ReplicationResponse{
			Data: []bft.QuorumRound{{
				Block: &testBlock{},
			}},
		},
	}, nodes[1])
	require.NoError(t, err)
}
