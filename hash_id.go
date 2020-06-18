package main

import (
	"bytes"
	"crypto/sha256"
	"math/big"
)

type HashID []byte

var (
	hash = sha256.New() // TODO: To be configurable
)

const (
	bitSize = 256
)

func NewHashID(key string) HashID {
	return hash.Sum([]byte(key))
}

func (h HashID) NextID() HashID {
	base := big.NewInt(0).SetBytes(h)
	return base.Add(base, big.NewInt(1)).Bytes()
}

func (h HashID) Between(from HashID, to HashID) bool {
	if from.GreaterThanEqual(to) {
		base := big.NewInt(0).SetBytes(h)
		offset := big.NewInt(0).Exp(big.NewInt(2), big.NewInt(bitSize), nil)
		to = base.Add(base, offset).Bytes()
	}
	return h.GreaterThan(from) && h.LessThan(to)
}

func (h HashID) Equals(other HashID) bool {
	return bytes.Compare(h, other) == 0
}

func (h HashID) LessThan(other HashID) bool {
	return bytes.Compare(h, other) < 0
}

func (h HashID) LessThanEqual(other HashID) bool {
	return h.Equals(other) || h.LessThan(other)
}

func (h HashID) GreaterThan(other HashID) bool {
	return bytes.Compare(h, other) > 0
}

func (h HashID) GreaterThanEqual(other HashID) bool {
	return h.Equals(other) || h.GreaterThan(other)
}
