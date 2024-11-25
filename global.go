// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "bytes"

type NodeID []byte

func (node NodeID) Equals(otherNode NodeID) bool {
	return bytes.Equal(node, otherNode)
}

type Bytes []byte
