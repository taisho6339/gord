package chord

import (
	"bytes"
	"github.com/taisho6339/gord/model"
	"math/big"
	"testing"
)

func TestNewFinger(t *testing.T) {
	testcases := []struct {
		id               model.HashID
		index            int
		expectedFingerID model.HashID
	}{
		{
			id:               big.NewInt(1).Bytes(),
			index:            0,
			expectedFingerID: big.NewInt(2).Bytes(),
		},
		{
			id:               big.NewInt(1).Bytes(),
			index:            1,
			expectedFingerID: big.NewInt(3).Bytes(),
		},
		{
			id:               big.NewInt(1).Bytes(),
			index:            2,
			expectedFingerID: big.NewInt(5).Bytes(),
		},
		{
			id:               big.NewInt(1).Bytes(),
			index:            256,
			expectedFingerID: big.NewInt(1).Bytes(),
		},
	}
	for _, testcase := range testcases {
		finger := NewFinger(testcase.id, testcase.index, nil)
		if bytes.Compare(finger.ID, testcase.expectedFingerID) != 0 {
			t.Fatalf("expected id %x, but actually %x", testcase.expectedFingerID, finger.ID)
		}
	}
}
