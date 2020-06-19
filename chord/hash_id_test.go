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
	testcases := []struct {
		from     int64
		to       int64
		target   int64
		expected bool
	}{
		{
			from:     1,
			to:       3,
			target:   2,
			expected: true,
		},
		{
			from:     3,
			to:       1,
			target:   2,
			expected: false,
		},
		{
			from:     3,
			to:       1,
			target:   4,
			expected: true,
		},
	}
	for _, testcase := range testcases {
		fromID := HashID(big.NewInt(testcase.from).Bytes())
		toID := HashID(big.NewInt(testcase.to).Bytes())
		targetID := HashID(big.NewInt(testcase.target).Bytes())
		if result := targetID.Between(fromID, toID); result != testcase.expected {
			t.Fatalf("Expected %v, but actually %v. The from is %v and to is %v.", testcase.expected, result, testcase.from, testcase.to)
		}
	}
}
