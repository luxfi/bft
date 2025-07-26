// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft_test

import (
	"testing"

	"github.com/luxfi/bft"
	"github.com/luxfi/bft/record"

	"github.com/stretchr/testify/require"
)

func newNotarization(logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) (bft.Notarization, error) {
	votesForCurrentRound := make(map[string]*bft.Vote)
	for _, id := range ids {
		vote, err := newTestVote(block, id)
		if err != nil {
			return bft.Notarization{}, err
		}

		votesForCurrentRound[string(id)] = vote
	}

	notarization, err := bft.NewNotarization(logger, signatureAggregator, votesForCurrentRound, block.BlockHeader())
	return notarization, err
}

func newNotarizationRecord(logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) ([]byte, error) {
	notarization, err := newNotarization(logger, signatureAggregator, block, ids)
	if err != nil {
		return nil, err
	}

	record := bft.NewQuorumRecord(notarization.QC.Bytes(), notarization.Vote.Bytes(), record.NotarizationRecordType)
	return record, nil
}

// creates a new finalization
func newFinalizationRecord(t *testing.T, logger bft.Logger, signatureAggregator bft.SignatureAggregator, block bft.VerifiedBlock, ids []bft.NodeID) (bft.Finalization, []byte) {
	finalizations := make([]*bft.FinalizeVote, len(ids))
	for i, id := range ids {
		finalizations[i] = newTestFinalizeVote(t, block, id)
	}

	finalization, err := bft.NewFinalization(logger, signatureAggregator, finalizations)
	require.NoError(t, err)

	record := bft.NewQuorumRecord(finalization.QC.Bytes(), finalization.Finalization.Bytes(), record.FinalizationRecordType)

	return finalization, record
}
