package server

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/chord"
	"github.com/taisho6339/gord/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"time"
)

type GordServer struct {
	chordClient    chord.Transport
	localChordNode *model.NodeRef
}

const (
	Port = "26041"
)

func NewGordServer(host string) *GordServer {
	return &GordServer{
		chordClient:    chord.NewChordApiClient(chord.NewLocalNode(host), time.Second),
		localChordNode: model.NewNodeRef(host, chord.ServerPort),
	}
}

func (g *GordServer) newGrpcServer() *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	RegisterGordServiceServer(s, g)
	return s
}

func (g *GordServer) Shutdown() {
	g.chordClient.Shutdown()
}

// Run runs chord server.
func (g *GordServer) Run(ctx context.Context) {
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%s", Port))
		if err != nil {
			log.Fatalf("failed to run gord server. reason: %#v", err)
		}
		grpcServer := g.newGrpcServer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to run gord server. reason: %#v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		g.Shutdown()
	}()
}

// FindHostForKey search for a given key's node.
// It is implemented for PublicService.
func (g *GordServer) FindHostForKey(ctx context.Context, req *FindHostRequest) (*Node, error) {
	id := model.NewHashID(req.Key)
	s, err := g.chordClient.FindSuccessorByTableRPC(ctx, g.localChordNode, id)
	if err != nil {
		log.Errorf("FindHostForKey failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: s.Reference().Host,
	}, nil
}
