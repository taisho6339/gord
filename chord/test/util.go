package test

import (
	"testing"
	"time"
)

func WaitCheckFuncWithTimeout(t *testing.T, f func() bool, duration time.Duration) {
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
		time.AfterFunc(duration, func() {
			timeout <- struct{}{}
		})
	}()
	select {
	case <-done:
		close(done)
		break
	case <-timeout:
		close(timeout)
		t.Fatal("test failed by timeout.")
	}
}
