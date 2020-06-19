package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	firstNode := CreateChordRing("127.0.0.1")
	secondNode := JoinNode("127.0.0.2", firstNode)
	thirdNode := JoinNode("192.168.10.1", firstNode)

	ctx, cancel := context.WithCancel(context.Background())
	firstNode.StartNode(ctx)
	secondNode.StartNode(ctx)
	thirdNode.StartNode(ctx)

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println(sig)
		done <- true
	}()

	go func() {
		http.HandleFunc("/find", func(writer http.ResponseWriter, request *http.Request) {
			values := request.URL.Query()
			if values == nil {
				writer.WriteHeader(400)
				return
			}
			host, valid := values["host"]
			if !valid {
				writer.WriteHeader(400)
				return
			}
			fmt.Println(fmt.Sprintf("Search for %s", host[0]))
			hashID := NewHashID(host[0])
			first := firstNode.FindSuccessorForFingerTable(hashID)
			second := secondNode.FindSuccessorForFingerTable(hashID)
			third := thirdNode.FindSuccessorForFingerTable(hashID)
			fmt.Println(fmt.Sprintf("Valid = %v", first.ID.Equals(second.ID) && first.ID.Equals(third.ID)))
			fmt.Println(fmt.Sprintf("NodeID = %x", first.ID))
			writer.WriteHeader(200)
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf(fmt.Sprintf("%#v", err))
		}
	}()

	fmt.Println("start chord server")
	select {
	case <-done:
		cancel()
		fmt.Println("end chord server")
	}
}
