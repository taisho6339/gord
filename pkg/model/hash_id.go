package model

import (
	"crypto/sha256"
	"math/big"
)

type HashID []byte

var (
	hashFunc = sha256.New // TODO: To be configuable
)

var (
	BitSize = 256
)

func NewHashID(key string) HashID {
	hf := hashFunc()
	hf.Write([]byte(key))
	return big.NewInt(0).Mod(big.NewInt(0).SetBytes(hf.Sum(nil)), big.NewInt(int64(BitSize))).Bytes()
}

func (h HashID) Add(offset int64) HashID {
	base := big.NewInt(0).SetBytes(h)
	return base.Add(base, big.NewInt(offset)).Bytes()
}

func (h HashID) Between(from HashID, to HashID) bool {
	if from.GreaterThanEqual(to) {
		return from.LessThan(h) || to.GreaterThan(h)
	}
	return h.GreaterThan(from) && h.LessThan(to)
}

func (h HashID) Equals(other HashID) bool {
	a := big.NewInt(0).SetBytes(h)
	b := big.NewInt(0).SetBytes(other)
	return a.Cmp(b) == 0
}

func (h HashID) LessThan(other HashID) bool {
	a := big.NewInt(0).SetBytes(h)
	b := big.NewInt(0).SetBytes(other)
	return a.Cmp(b) < 0
}

func (h HashID) LessThanEqual(other HashID) bool {
	return h.Equals(other) || h.LessThan(other)
}

func (h HashID) GreaterThan(other HashID) bool {
	a := big.NewInt(0).SetBytes(h)
	b := big.NewInt(0).SetBytes(other)
	return a.Cmp(b) > 0
}

func (h HashID) GreaterThanEqual(other HashID) bool {
	return h.Equals(other) || h.GreaterThan(other)
}
