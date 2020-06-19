package chord

import (
	"math/big"
	"testing"
)

func TestNewHashID(t *testing.T) {
	host := "127.0.0.1"
	a := NewHashID(host)
	b := NewHashID(host)
	if !a.Equals(b) {
		t.Fatalf("a is not b. a = %x, b = %x", a, b)
	}
}

func TestBetween(t *testing.T) {
	a := HashID(big.NewInt(1).Bytes())
	b := HashID(big.NewInt(3).Bytes())
	target := HashID(big.NewInt(2).Bytes())
	if !target.Between(a, b) {
		t.Fatalf("Expected true, but actually false. The from is 1 and to is 3.")
	}
	if !target.Between(b, a) {
		t.Fatalf("Expected true, but actually false. The from is 3 and to is 1.")
	}
}
