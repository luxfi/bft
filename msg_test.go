package bft_test

import (
	"testing"

	"github.com/luxfi/bft"

	"github.com/stretchr/testify/require"
)

func TestQuorumRoundMalformed(t *testing.T) {
	tests := []struct {
		name        string
		qr          bft.QuorumRound
		expectedErr bool
	}{
		{
			name: "empty notarization",
			qr: bft.QuorumRound{
				EmptyNotarization: &bft.EmptyNotarization{},
			},
			expectedErr: false,
		}, {
			name: "all nil",
			qr: bft.QuorumRound{
				EmptyNotarization: nil,
				Block:             nil,
				Notarization:      nil,
				Finalization:      nil,
			},
			expectedErr: true,
		}, {
			name: "block and notarization",
			qr: bft.QuorumRound{
				Block:        &testBlock{},
				Notarization: &bft.Notarization{},
			},
			expectedErr: false,
		}, {
			name: "block and finalization",
			qr: bft.QuorumRound{
				Block:        &testBlock{},
				Finalization: &bft.Finalization{},
			},
			expectedErr: false,
		}, {
			name: "block and empty notarization",
			qr: bft.QuorumRound{
				Block:             &testBlock{},
				EmptyNotarization: &bft.EmptyNotarization{},
			},
			expectedErr: true,
		},
		{
			name: "block and notarization and finalization",
			qr: bft.QuorumRound{
				Block:        &testBlock{},
				Notarization: &bft.Notarization{},
				Finalization: &bft.Finalization{},
			},
			expectedErr: false,
		},
		{
			name: "notarization and no block",
			qr: bft.QuorumRound{
				Notarization: &bft.Notarization{},
			},
			expectedErr: true,
		},
		{
			name: "finalization and no block",
			qr: bft.QuorumRound{
				Finalization: &bft.Finalization{},
			},
			expectedErr: true,
		},
		{
			name: "just block",
			qr: bft.QuorumRound{
				Block: &testBlock{},
			},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.qr.IsWellFormed()
			if err != nil {
				require.True(t, test.expectedErr)
				return
			}
			require.False(t, test.expectedErr)
		})
	}

}
