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

	ctx, cancel := context.WithCancel(context.Background())
	firstNode.StartNode(ctx)
	secondNode.StartNode(ctx)

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
			//queries := request.URL.Query()
			//id, ok := queries["id"]
			//if !ok {
			//	writer.WriteHeader(200)
			//	return
			//}
			hashID := NewHashID("127.0.0.2")
			fmt.Println(fmt.Sprintf("hash id = %x", hashID))
			fmt.Println(fmt.Sprintf("first node id = %x", firstNode.ID))
			fmt.Println(fmt.Sprintf("second node id = %x", secondNode.ID))

			foundNode := firstNode.FindSuccessor(hashID)
			fmt.Println(fmt.Sprintf("found node = %x", foundNode.ID))
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
