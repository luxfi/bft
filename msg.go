// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

type Message struct {
	BlockMessage BlockMessage
}

type BlockMessage struct {
	Block Block
}
