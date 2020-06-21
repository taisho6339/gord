package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/chord"
	"github.com/taisho6339/gord/server"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	sigs      = make(chan os.Signal, 1)
	done      = make(chan bool, 1)
	host      = os.Getenv("NODE_HOST")
	existHost = os.Getenv("EXIST_NODE_HOST")
)

func runChordServer(ctx context.Context) {
	opts := []chord.ServerOptionFunc{
		chord.WithNodeOption(host),
		chord.WithTimeoutConnNode(time.Second * 5),
	}
	if existHost != "" {
		opts = append(opts, chord.WithProcessOptions(chord.WithExistNode(existHost)))
	}
	cs := chord.NewChordServer(opts...)
	cs.Run(ctx)
	log.Info("Running Chord server...")
	log.Infof("Chord listening on %s:%s", host, chord.ServerPort)
}

func runGordServer(ctx context.Context) {
	gs := server.NewGordServer(host)
	gs.Run(ctx)
	log.Info("Running Gord server...")
	log.Infof("Gord is listening on %s:%s", host, server.Port)
}

func main() {
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	if host == "" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithCancel(context.Background())
	runChordServer(ctx)
	runGordServer(ctx)
	<-done
	cancel()
}
