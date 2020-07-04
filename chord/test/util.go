package test

import (
	"runtime"
	"testing"
	"time"
)

// PanicFail make test be failure if panic occurs.
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

func WaitCheckFuncWithTimeout(t *testing.T, f func() bool, duration time.Duration) {
	done := make(chan struct{}, 1)
	defer close(done)

	go func() {
		for {
			if f() {
				done <- struct{}{}
				return
			}
		}
	}()
	go func() {
		time.AfterFunc(duration, func() {
			close(done)
		})
	}()
	select {
	case _, ok := <-done:
		if !ok {
			t.Fatal("test failed by timeout.")
		}
	}
}
