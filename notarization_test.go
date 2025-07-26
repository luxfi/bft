// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/luxfi/bft"
	"github.com/luxfi/bft/testutil"

	"github.com/stretchr/testify/require"
)

var errorSigAggregation = errors.New("signature error")

func TestNewNotarization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	testBlock := &testBlock{}
	tests := []struct {
		name                 string
		votesForCurrentRound map[string]*bft.Vote
		block                bft.VerifiedBlock
		expectError          error
		signatureAggregator  bft.SignatureAggregator
	}{
		{
			name: "valid notarization",
			votesForCurrentRound: func() map[string]*bft.Vote {
				votes := make(map[string]*bft.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := newTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &testSignatureAggregator{},
			expectError:         nil,
		},
		{
			name:                 "no votes",
			votesForCurrentRound: map[string]*bft.Vote{},
			block:                testBlock,
			signatureAggregator:  &testSignatureAggregator{},
			expectError:          bft.ErrorNoVotes,
		},
		{
			name: "error aggregating",
			votesForCurrentRound: func() map[string]*bft.Vote {
				votes := make(map[string]*bft.Vote)
				nodeIds := [][]byte{{1}, {2}, {3}, {4}, {5}}
				for _, nodeId := range nodeIds {
					vote, err := newTestVote(testBlock, nodeId)
					require.NoError(t, err)
					votes[string(nodeId)] = vote
				}
				return votes
			}(),
			block:               testBlock,
			signatureAggregator: &testSignatureAggregator{err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notarization, err := bft.NewNotarization(l, tt.signatureAggregator, tt.votesForCurrentRound, tt.block.BlockHeader())
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				signers := notarization.QC.Signers()
				require.Equal(t, len(signers), len(tt.votesForCurrentRound), "incorrect amount of signers")

				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}

}

func TestNewFinalization(t *testing.T) {
	l := testutil.MakeLogger(t, 1)
	tests := []struct {
		name                 string
		finalizeVotes        []*bft.FinalizeVote
		signatureAggregator  bft.SignatureAggregator
		expectedFinalization *bft.ToBeSignedFinalization
		expectedQC           *bft.QuorumCertificate
		expectError          error
	}{
		{
			name: "valid finalizations in order",
			finalizeVotes: []*bft.FinalizeVote{
				newTestFinalizeVote(t, &testBlock{}, []byte{1}),
				newTestFinalizeVote(t, &testBlock{}, []byte{2}),
				newTestFinalizeVote(t, &testBlock{}, []byte{3}),
			},
			signatureAggregator:  &testSignatureAggregator{},
			expectedFinalization: &newTestFinalizeVote(t, &testBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "unsorted finalizations",
			finalizeVotes: []*bft.FinalizeVote{
				newTestFinalizeVote(t, &testBlock{}, []byte{3}),
				newTestFinalizeVote(t, &testBlock{}, []byte{1}),
				newTestFinalizeVote(t, &testBlock{}, []byte{2}),
			},
			signatureAggregator:  &testSignatureAggregator{},
			expectedFinalization: &newTestFinalizeVote(t, &testBlock{}, []byte{1}).Finalization,
			expectError:          nil,
		},
		{
			name: "finalizations with different digests",
			finalizeVotes: []*bft.FinalizeVote{
				newTestFinalizeVote(t, &testBlock{digest: [32]byte{1}}, []byte{1}),
				newTestFinalizeVote(t, &testBlock{digest: [32]byte{2}}, []byte{2}),
				newTestFinalizeVote(t, &testBlock{digest: [32]byte{3}}, []byte{3}),
			},
			signatureAggregator: &testSignatureAggregator{},
			expectError:         bft.ErrorInvalidFinalizationDigest,
		},
		{
			name: "signature aggregator errors",
			finalizeVotes: []*bft.FinalizeVote{
				newTestFinalizeVote(t, &testBlock{}, []byte{1}),
			},
			signatureAggregator: &testSignatureAggregator{err: errorSigAggregation},
			expectError:         errorSigAggregation,
		},
		{
			name:                "no votes",
			finalizeVotes:       []*bft.FinalizeVote{},
			signatureAggregator: &testSignatureAggregator{},
			expectError:         bft.ErrorNoVotes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finalization, err := bft.NewFinalization(l, tt.signatureAggregator, tt.finalizeVotes)
			require.ErrorIs(t, err, tt.expectError, "expected error, got nil")

			if tt.expectError == nil {
				require.Equal(t, finalization.Finalization.Digest, tt.expectedFinalization.Digest, "digests not correct")

				signers := finalization.QC.Signers()
				require.Equal(t, len(signers), len(tt.finalizeVotes), "unexpected amount of signers")

				// ensure the qc signers are in order
				for i, signer := range signers[1:] {
					require.Negative(t, bytes.Compare(signers[i], signer), "signers not in order")
				}
			}
		})
	}
}
