package model

import (
	"bytes"
	"crypto/sha256"
	"math/big"
)

type HashID []byte

var (
	hashFunc = sha256.New // TODO: To be configuable
)

func NewHashID(key string) HashID {
	hf := hashFunc()
	hf.Write([]byte(key))
	return hf.Sum(nil)
}

// Size returns bit size of hash id.
func (h HashID) Size() int {
	return len(h) * 8 //bit
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
