package gordserver

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
		timeoutConnNode: time.Second * 5,
	}
}

func WithTimeoutConnNode(duration time.Duration) ChordOption {
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
	RegisterChordInternalServiceServer(s, cs)
	RegisterChordServiceServer(s, cs)
	return s
}

func (cs *ChordServer) Successor(_ context.Context, _ *empty.Empty) (*Node, error) {
	succ := cs.process.Node.Successor
	if succ == nil {
		return nil, status.Errorf(codes.Internal, "server: internal error occured. successor is not set.")
	}
	return &Node{
		Host: succ.Host,
		Port: succ.Port,
	}, nil
}

func (cs *ChordServer) Predecessor(_ context.Context, _ *empty.Empty) (*Node, error) {
	pred := cs.process.Node.Predecessor
	if pred != nil {
		return &Node{
			Host: pred.Host,
			Port: pred.Port,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "server: predecessor is not set.")
}

func (cs *ChordServer) FindSuccessor(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessor(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Host,
		Port: successor.Port,
	}, nil
}

func (cs *ChordServer) FindSuccessorFallback(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessorFallback(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor fallback failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Host,
		Port: successor.Port,
	}, nil
}

func (cs *ChordServer) FindClosestPrecedingNode(_ context.Context, req *FindRequest) (*Node, error) {
	node, err := cs.process.Node.FindClosestPrecedingNode(req.Id)
	if err == chord.ErrStabilizeNotCompleted {
		return nil, status.Error(codes.NotFound, "Stabilize not completed.")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find closest preceding node failed. reason = %#v", err)
	}
	return &Node{
		Host: node.Host,
		Port: node.Port,
	}, nil
}

func (cs *ChordServer) Notify(_ context.Context, req *Node) (*empty.Empty, error) {
	err := cs.process.Node.Notify(chord.NewNodeRef(req.Host, req.Port))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: notify failed. reason = %#v", err)
	}
	return &empty.Empty{}, nil
}

// FindHostForKey search for a given key's node.
// It is implemented for PublicService.
func (cs *ChordServer) FindHostForKey(ctx context.Context, req *FindHostRequest) (*Node, error) {
	id := chord.NewHashID(req.Key)
	s, err := cs.process.Node.FindSuccessor(ctx, id)
	if err != nil {
		log.Errorf("FindHostForKey failed. reason: %#v", err)
		return nil, err
	}
	return &Node{
		Host: s.Host,
		Port: s.Port,
	}, nil
}

// Run runs chord server.
func (cs *ChordServer) Run(ctx context.Context) {
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cs.opt.host, cs.opt.port))
		if err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		grpcServer := cs.newGrpcServer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
	}()
	go func() {
		if err := cs.process.Start(ctx); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		<-ctx.Done()
	}()
}

func (cs *ChordServer) Shutdown() {
	cs.process.Shutdown()
}
