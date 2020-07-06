package server

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/chord"
	"github.com/taisho6339/gord/pkg/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type ExternalServer struct {
	process *chord.Process
}

func NewExternalServer(process *chord.Process) *ExternalServer {
	return &ExternalServer{
		process: process,
	}
}

func (g *ExternalServer) newGrpcServer() *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	RegisterExternalServiceServer(s, g)
	return s
}

// Run runs chord server.
func (g *ExternalServer) Run() {
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%s", ExternalServerPort))
		if err != nil {
			log.Fatalf("failed to run gord server. reason: %#v", err)
		}
		grpcServer := g.newGrpcServer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to run gord server. reason: %#v", err)
		}
	}()
}

// FindHostForKey search for a given key's node.
// It is implemented for PublicService.
func (g *ExternalServer) FindHostForKey(ctx context.Context, req *FindHostRequest) (*Node, error) {
	id := model.NewHashID(req.Key)
	s, err := g.process.FindSuccessorByTable(ctx, id)
	if err != nil {
		log.Errorf("FindHostForKey failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: s.Reference().Host,
	}, nil
}
