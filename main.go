package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/gordserver"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cs := gordserver.NewChordServer(
		gordserver.WithHostOption("127.0.0.1"),
		gordserver.WithPortOption(8080),
	)
	log.Infof("Starting Chord Process...")
	if err := cs.Run(ctx); err != nil {
		log.Fatalf("failed to chord server. reason: %#v", err)
	}
	log.Infof("Start serving by grpc")
	select {
	case <-done:
		log.Infof("Ending Chord Process...")
		cancel()
	}
}
