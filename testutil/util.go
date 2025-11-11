// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"encoding/asn1"
	"testing"

	"github.com/luxfi/bft"
	"github.com/stretchr/testify/require"
)

type TestSignatureAggregator struct {
	Err error
}

func (t *TestSignatureAggregator) Aggregate(signatures []bft.Signature) (bft.QuorumCertificate, error) {
	return TestQC(signatures), t.Err
}

type TestQC []bft.Signature

func (t TestQC) Signers() []bft.NodeID {
	res := make([]bft.NodeID, 0, len(t))
	for _, sig := range t {
		res = append(res, sig.Signer)
	}
	return res
}

func (t TestQC) Verify(msg []byte) error {
	return nil
}

func (t TestQC) Bytes() []byte {
	bytes, err := asn1.Marshal(t)
	if err != nil {
		panic(err)
	}
	return bytes
}

type AnyBlock interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() bft.BlockHeader
}

func NewTestVote(block AnyBlock, id bft.NodeID) (*bft.Vote, error) {
	vote := bft.ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(&testSigner{})
	if err != nil {
		return nil, err
	}

	return &bft.Vote{
		Signature: bft.Signature{
			Signer: id,
			Value:  sig,
		},
		Vote: vote,
	}, nil
}

func NewTestFinalizeVote(t *testing.T, block bft.VerifiedBlock, id bft.NodeID) *bft.FinalizeVote {
	f := bft.ToBeSignedFinalization{BlockHeader: block.BlockHeader()}
	sig, err := f.Sign(&testSigner{})
	require.NoError(t, err)
	return &bft.FinalizeVote{
		Signature: bft.Signature{
			Signer: id,
			Value:  sig,
		},
		Finalization: bft.ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

type testSigner struct {
}

func (t *testSigner) Sign([]byte) ([]byte, error) {
	return []byte{1, 2, 3}, nil
}

type testVerifier struct {
}

func (t *testVerifier) VerifyBlock(bft.VerifiedBlock) error {
	return nil
}

func (t *testVerifier) Verify(_ []byte, _ []byte, _ bft.NodeID) error {
	return nil
}
