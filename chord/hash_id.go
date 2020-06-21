package chord

import (
	"bytes"
	"crypto/sha256"
	"math/big"
)

type HashID []byte

var (
	hashFunc = sha256.New
)

const (
	bitSize = 256 // TODO: Get by Size function
)

func NewHashID(key string) HashID {
	hf := hashFunc()
	hf.Write([]byte(key))
	return hf.Sum(nil)
}

func (h HashID) NextID() HashID {
	base := big.NewInt(0).SetBytes(h)
	return base.Add(base, big.NewInt(1)).Bytes()
}

func (h HashID) Between(from HashID, to HashID) bool {
	if from.GreaterThanEqual(to) {
		return from.LessThan(h) || to.GreaterThan(h)
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