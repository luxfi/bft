// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"testing"

	"github.com/luxfi/bft"
	"github.com/luxfi/bft/record"

	"github.com/stretchr/testify/require"
)

// NewEmptyNotarization creates a new empty notarization
func NewEmptyNotarization(nodes []bft.NodeID, round uint64, seq uint64) *bft.EmptyNotarization {
	var qc TestQC

	for i, node := range nodes {
		qc = append(qc, bft.Signature{Signer: node, Value: []byte{byte(i)}})
	}

	return &bft.EmptyNotarization{
		QC: qc,
		Vote: bft.ToBeSignedEmptyVote{ProtocolMetadata: bft.ProtocolMetadata{
			Round: round,
			Seq:   seq,
		}},
	}
}

func NewNotarization(logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) (bft.Notarization, error) {
	votesForCurrentRound := make(map[string]*bft.Vote)
	for _, id := range ids {
		vote, err := NewTestVote(block, id)
		if err != nil {
			return bft.Notarization{}, err
		}

		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := bft.NewNotarization(logger, signatureAggregator, votesForCurrentRound, block.BlockHeader())
	return notarization, err
}

func NewNotarizationRecord(logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) ([]byte, error) {
	notarization, err := NewNotarization(logger, signatureAggregator, block, ids)
	if err != nil {
		return nil, err
	}

	record := bft.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)
	return record, nil
}

// creates a new finalization
func NewFinalizationRecord(t *testing.T, logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) (bft.Finalization, []byte) {
	finalizations := make([]*bft.FinalizeVote, len(ids))
	for i, id := range ids {
		finalizations[i] = NewTestFinalizeVote(t, block, id)
	}

	finalization, err := bft.NewFinalization(logger, signatureAggregator, finalizations)
	require.NoError(t, err)

	record := bft.NewQuorumRecord(finalization.QC.Bytes(), finalization.Finalization.Bytes(), record.FinalizationRecordType)

	return finalization, record
}
