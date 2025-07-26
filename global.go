// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bft

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type NodeID []byte

func (node NodeID) String() string {
	var nodePrint [8]byte
	copy(nodePrint[:], node)
	return hex.EncodeToString(nodePrint[:])
}

func (node NodeID) Equals(otherNode NodeID) bool {
	return bytes.Equal(node, otherNode)
}

type NodeIDs []NodeID

func (nodes NodeIDs) String() string {
	var nodeStrings []string
	for _, node := range nodes {
		nodeStrings = append(nodeStrings, node.String())
	}
	return fmt.Sprintf("%v", nodeStrings)
}

func (nodes NodeIDs) Remove(targetNode NodeID) []NodeID {
	for i, n := range nodes {
		if n.Equals(targetNode) {
			result := make([]NodeID, 0, len(nodes)-1)
			result = append(result, nodes[:i]...)
			result = append(result, nodes[i+1:]...)
			return result
		}
	}
	return nodes
}
