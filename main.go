package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/gordserver"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	sigs = make(chan os.Signal, 1)
	done = make(chan bool, 1)
)

func main() {
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	log.Infof("Starting gord server.")
	ctx, cancel := context.WithCancel(context.Background())
	cs := gordserver.NewChordServer(
		gordserver.WithHostOption("127.0.0.1"),
		gordserver.WithPortOption(8080),
		gordserver.WithTimeoutConnNode(time.Second*5),
	)
	cs.Run(ctx)
	log.Infof("Gord is Ready.")
	<-done
	cs.Shutdown()
	log.Infof("Stopping Gord server.")
	cancel()
}
