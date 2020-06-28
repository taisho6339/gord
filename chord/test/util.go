package test

import (
	"runtime"
	"testing"
)

func PanicFail(t *testing.T) {
	if val := recover(); val != nil {
		for depth := 0; ; depth++ {
			_, file, line, ok := runtime.Caller(depth)
			if !ok {
				break
			}
			t.Logf("%v:%d", file, line)
		}
		t.Fatalf("panic error = %#v", val)
	}
}