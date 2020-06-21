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
	sigs      = make(chan os.Signal, 1)
	done      = make(chan bool, 1)
	host      = os.Getenv("NODE_HOST")
	port      = os.Getenv("NODE_PORT")
	existHost = os.Getenv("EXIST_NODE_HOST")
	existPort = os.Getenv("EXIST_NODE_PORT")
)

func main() {
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	log.Infof("Starting gord server.")
	ctx, cancel := context.WithCancel(context.Background())
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "8080"
	}
	opts := []gordserver.ChordOptionFunc{
		gordserver.WithNodeOption(host, port),
		gordserver.WithTimeoutConnNode(time.Second * 5),
	}
	if existHost != "" {
		opts = append(opts, gordserver.WithExistNode(existHost, existPort))
	}
	cs := gordserver.NewChordServer(opts...)
	cs.Run(ctx)

	log.Infof("Gord is Ready.")
	<-done
	cs.Shutdown()
	log.Infof("Stopping Gord server.")
	cancel()
}
