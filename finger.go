package main

import (
	"math/big"
)

type Finger struct {
	ID   HashID
	Node *ServerNode
}

func NewFinger(source *ServerNode, i int, successor *ServerNode) *Finger {
	nodeID := big.NewInt(0).SetBytes(source.ID)
	base := big.NewInt(2)

	offset := base.Exp(base, big.NewInt(int64(i)), nil) // 2^i
	sum := nodeID.Add(nodeID, offset)                   // n + 2^i
	mod := base.Exp(base, big.NewInt(bitSize), nil)     // (n + 2^i) mod 2^m
	fingerID := sum.Mod(sum, mod)

	return &Finger{
		ID:   fingerID.Bytes(),
		Node: successor,
	}
}
