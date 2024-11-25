// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

type InMemWAL struct {
	Data []*Record
}

func (wal *InMemWAL) Append(record *Record) {
	wal.Data = append(wal.Data, record)
}

func (wal *InMemWAL) ReadAll() []Record {
	res := make([]Record, len(wal.Data))
	for i, record := range wal.Data {
		res[i] = *record
	}
	return res
}
