package chord

import (
	"math/big"
)

type Finger struct {
	ID   HashID
	Node *NodeRef
}

// NewFinger creates a finger.
// index is an order of finger table.
// node is this finger table's owner.
func NewFinger(id HashID, index int, successor *NodeRef) *Finger {
	nodeID := big.NewInt(0).SetBytes(id)
	base := big.NewInt(2)

	offset := base.Exp(base, big.NewInt(int64(index)), nil) // 2^i
	sum := nodeID.Add(nodeID, offset)                       // n + 2^i
	mod := base.Exp(base, big.NewInt(bitSize), nil)         // (n + 2^i) mod 2^m
	fingerID := sum.Mod(sum, mod)

	return &Finger{
		ID:   fingerID.Bytes(),
		Node: successor,
	}
}
