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

func WaitForFunc(t *testing.T, f func() bool) {
	done := make(chan struct{}, 1)
	timeout := make(chan struct{}, 1)
	go func() {
		for {
			if f() {
				done <- struct{}{}
				return
			}
		}
	}()
	go func() {
		time.AfterFunc(time.Second*10, func() {
			timeout <- struct{}{}
		})
	}()
	select {
	case <-done:
		break
	case <-timeout:
		t.Fatal("test failed by timeout.")
	}
}
