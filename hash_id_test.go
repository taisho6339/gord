package main

import (
	"fmt"
	"testing"
)

func TestNewHashID(t *testing.T) {
	host := "127.0.0.1"
	a := NewHashID(host)
	b := NewHashID("")
	fmt.Println(fmt.Sprintf("a is %x", a))
	if !a.Equals(b) {
		t.Fatalf("a is not b. a = %x, b = %x", a, b)
	}
}
