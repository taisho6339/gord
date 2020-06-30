package server

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/chord"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"net"
	"time"
)

type InternalServer struct {
	process *chord.Process
	opt     *chordOption
}

type chordOption struct {
	host            string
	timeoutConnNode time.Duration
	processOpts     []chord.ProcessOptionFunc
}

type InternalServerOptionFunc func(option *chordOption)

func newDefaultServerOption() *chordOption {
	return &chordOption{
		host:            "127.0.0.1",
		timeoutConnNode: time.Second * 5,
	}
}

func WithNodeOption(host string) InternalServerOptionFunc {
	return func(option *chordOption) {
		option.host = host
	}
}

func WithProcessOptions(opts ...chord.ProcessOptionFunc) InternalServerOptionFunc {
	return func(option *chordOption) {
		option.processOpts = append(option.processOpts, opts...)
	}
}

func WithTimeoutConnNode(duration time.Duration) InternalServerOptionFunc {
	return func(option *chordOption) {
		option.timeoutConnNode = duration
	}
}

func NewChordServer(process *chord.Process, opts ...InternalServerOptionFunc) *InternalServer {
	opt := newDefaultServerOption()
	for _, o := range opts {
		o(opt)
	}
	return &InternalServer{
		process: process,
		opt:     opt,
	}
}

func (is *InternalServer) newGrpcServer() *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	RegisterInternalServiceServer(s, is)
	return s
}

// Run runs chord server.
func (is *InternalServer) Run(ctx context.Context) {
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", is.opt.host, ServerPort))
		if err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		grpcServer := is.newGrpcServer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
	}()
	go func() {
		if err := is.process.Start(ctx, is.opt.processOpts...); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		<-ctx.Done()
		is.process.Shutdown()
	}()
}

func (is *InternalServer) Ping(_ context.Context, _ *empty.Empty) (*empty.Empty, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	return &empty.Empty{}, nil
}

func (is *InternalServer) Successors(ctx context.Context, req *empty.Empty) (*Nodes, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	successors, err := is.process.GetSuccessors(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: internal error occured. successor is not set.")
	}
	var nodes []*Node
	for _, suc := range successors {
		if suc == nil {
			continue
		}
		nodes = append(nodes, &Node{
			Host: suc.Reference().Host,
		})
	}
	return &Nodes{
		Nodes: nodes,
	}, nil
}

func (is *InternalServer) Predecessor(ctx context.Context, _ *empty.Empty) (*Node, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	pred, err := is.process.GetPredecessor(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: internal error occured. predecessor is not set.")
	}
	if pred != nil {
		return &Node{
			Host: pred.Reference().Host,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "server: predecessor is not set.")
}

func (is *InternalServer) FindSuccessorByTable(ctx context.Context, req *FindRequest) (*Node, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	successor, err := is.process.FindSuccessorByTable(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Reference().Host,
	}, nil
}

func (is *InternalServer) FindSuccessorByList(ctx context.Context, req *FindRequest) (*Node, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	successor, err := is.process.FindSuccessorByList(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor fallback failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Reference().Host,
	}, nil
}

func (is *InternalServer) FindClosestPrecedingNode(ctx context.Context, req *FindRequest) (*Node, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	node, err := is.process.FindClosestPrecedingNode(ctx, req.Id)
	if err == chord.ErrStabilizeNotCompleted {
		return nil, status.Error(codes.NotFound, "Stabilize not completed.")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find closest preceding node failed. reason = %#v", err)
	}
	return &Node{
		Host: node.Reference().Host,
	}, nil
}

func (is *InternalServer) Notify(ctx context.Context, req *Node) (*empty.Empty, error) {
	if is.process.IsShutdown {
		return nil, status.Errorf(codes.Unavailable, "server has started shutdown")
	}
	err := is.process.Notify(ctx, chord.NewRemoteNode(req.Host, is.process.Transport))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: notify failed. reason = %#v", err)
	}
	return &empty.Empty{}, nil
}
