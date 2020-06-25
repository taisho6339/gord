package chord

import (
	"github.com/taisho6339/gord/model"
	"math/big"
)

type Finger struct {
	Index int
	ID    model.HashID
	Node  RingNode
}

// NewFingerTable creates a finger table.
func NewFingerTable(id model.HashID) []*Finger {
	table := make([]*Finger, id.Size())
	for i := range table {
		table[i] = NewFinger(id, i, nil)
	}
	return table
}

// NewFinger creates a finger.
// index is an order of finger table.
// node is this finger table's owner.
func NewFinger(id model.HashID, index int, successor RingNode) *Finger {
	nodeID := big.NewInt(0).SetBytes(id)
	base := big.NewInt(2)

	offset := base.Exp(base, big.NewInt(int64(index)), nil)  // 2^i
	sum := nodeID.Add(nodeID, offset)                        // n + 2^i
	mod := base.Exp(base, big.NewInt(int64(id.Size())), nil) // (n + 2^i) mod 2^m
	fingerID := sum.Mod(sum, mod)

	return &Finger{
		Index: index,
		ID:    fingerID.Bytes(),
		Node:  successor,
	}
}
