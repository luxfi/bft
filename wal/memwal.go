// Copyright (C) 2019-2024, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type InMemWAL struct {
	bb bytes.Buffer
	t  *testing.T
}

func NewMemWAL(t *testing.T) *InMemWAL {
	return &InMemWAL{
		t: t,
	}
}

func (wal *InMemWAL) Append(b []byte) error {
	w := &wal.bb
	err := writeRecord(w, b)
	require.NoError(wal.t, err)
	return err
}

func (wal *InMemWAL) ReadAll() ([][]byte, error) {
	r := bytes.NewBuffer(wal.bb.Bytes())
	var res [][]byte
	for r.Len() > 0 {
		payload, _, err := readRecord(r, uint32(r.Len()))
		if err != nil {
			return nil, fmt.Errorf("failed reading in-memory record: %w", err)
		}
		res = append(res, payload)
	}
	return res, nil
}
