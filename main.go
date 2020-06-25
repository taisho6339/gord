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

func runChordServer(ctx context.Context, process *chord.Process) {
	opts := []server.ServerOptionFunc{
		server.WithNodeOption(host),
		server.WithTimeoutConnNode(time.Second * 5),
	}
	if existHost != "" {
		opts = append(opts, server.WithProcessOptions(chord.WithExistNode(existHost)))
	}
	cs := server.NewChordServer(process, opts...)
	cs.Run(ctx)
	log.Info("Running Chord server...")
	log.Infof("Chord listening on %s:%s", host, server.ServerPort)
}

func runGordServer(process *chord.Process) {
	gs := server.NewExternalServer(process)
	gs.Run()
	log.Info("Running Gord server...")
	log.Infof("Gord is listening on %s:%s", host, server.ExternalServerPort)
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
	defer cancel()
	localNode := chord.NewLocalNode(host)
	transport := server.NewChordApiClient(localNode, time.Second*3)
	process := chord.NewProcess(localNode, transport)
	runChordServer(ctx, process)
	runGordServer(process)
	<-done
}
