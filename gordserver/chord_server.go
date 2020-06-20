package gordserver

import (
	"context"
	"errors"
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

type ChordServer struct {
	process *chord.Process
	opt     *chordOption
}

type chordOption struct {
	host            string
	port            int
	timeoutConnNode time.Duration
}

type ChordOption func(option *chordOption)

func newDefaultOption() *chordOption {
	return &chordOption{
		host:            "127.0.0.1",
		port:            8080,
		timeoutConnNode: time.Minute * 1,
	}
}

func TimeoutConnNode(duration time.Duration) ChordOption {
	return func(option *chordOption) {
		option.timeoutConnNode = duration
	}
}

func WithHostOption(host string) ChordOption {
	return func(option *chordOption) {
		option.host = host
	}
}

func WithPortOption(port int) ChordOption {
	return func(option *chordOption) {
		option.port = port
	}
}

func NewChordServer(opts ...ChordOption) *ChordServer {
	opt := newDefaultOption()
	for _, o := range opts {
		o(opt)
	}
	return &ChordServer{
		process: chord.NewProcess(opt.host, fmt.Sprintf("%d", opt.port), NewChordApiClient(opt.timeoutConnNode)),
		opt:     opt,
	}
}

func (cs *ChordServer) newGrpcServer() *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	RegisterChordServiceServer(s, cs)
	return s
}

// Run runs chord server.
func (cs *ChordServer) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cs.opt.host, cs.opt.port))
	if err != nil {
		return errors.New(fmt.Sprintf("failed to run chord server. reason: %#v", err))
	}
	if err := cs.process.StartProcess(ctx); err != nil {
		return errors.New(fmt.Sprintf("failed to run chord server. reason: %#v", err))
	}
	grpcServer := cs.newGrpcServer()
	if err := grpcServer.Serve(lis); err != nil {
		return errors.New(fmt.Sprintf("failed to run chord server. reason: %#v", err))
	}
	return nil
}

func (cs *ChordServer) Successor(ctx context.Context, req *empty.Empty) (*Node, error) {
	succ := cs.process.Node.Successor
	if succ != nil {
		return &Node{
			Host: succ.Host,
			Port: succ.Port,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "successor is not set")
}

func (cs *ChordServer) Predecessor(ctx context.Context, req *empty.Empty) (*Node, error) {
	pred := cs.process.Node.Predecessor
	if pred != nil {
		return &Node{
			Host: pred.Host,
			Port: pred.Port,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "predecessor is not set")
}

func (cs *ChordServer) FindSuccessor(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessor(ctx, req.Id)
	if err != nil {
		log.Errorf("find successor failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: successor.Host,
		Port: successor.Port,
	}, nil
}

func (cs *ChordServer) FindSuccessorFallback(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessorFallback(ctx, req.Id)
	if err != nil {
		log.Errorf("find successor fallback failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: successor.Host,
		Port: successor.Port,
	}, nil
}

func (cs *ChordServer) FindPredecessor(ctx context.Context, req *FindRequest) (*Node, error) {
	predecessor, err := cs.process.Node.FindPredecessor(ctx, req.Id)
	if err != nil {
		log.Errorf("find predecessor failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: predecessor.Host,
		Port: predecessor.Port,
	}, nil
}

func (cs *ChordServer) FindClosestPrecedingNode(ctx context.Context, req *FindRequest) (*Node, error) {
	node, err := cs.process.Node.FindClosestPrecedingNode(req.Id)
	if err != nil {
		log.Errorf("find closest preceding node failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: node.Host,
		Port: node.Port,
	}, nil
}

func (cs *ChordServer) Notify(ctx context.Context, req *Node) (*empty.Empty, error) {
	err := cs.process.Node.Notify(chord.NewNodeRef(req.Host, req.Port))
	if err != nil {
		log.Errorf("notify failed. reason: %#v", err)
		return nil, err
	}
	return &empty.Empty{}, nil
}
