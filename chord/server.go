package chord

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"net"
	"time"
)

type Server struct {
	process *Process
	opt     *chordOption
}

type chordOption struct {
	host            string
	timeoutConnNode time.Duration
	processOpts     []ProcessOptionFunc
}

type ServerOptionFunc func(option *chordOption)

func newDefaultServerOption() *chordOption {
	return &chordOption{
		host:            "127.0.0.1",
		timeoutConnNode: time.Second * 5,
	}
}

func WithNodeOption(host string) ServerOptionFunc {
	return func(option *chordOption) {
		option.host = host
	}
}

func WithProcessOptions(opts ...ProcessOptionFunc) ServerOptionFunc {
	return func(option *chordOption) {
		option.processOpts = append(option.processOpts, opts...)
	}
}

func WithTimeoutConnNode(duration time.Duration) ServerOptionFunc {
	return func(option *chordOption) {
		option.timeoutConnNode = duration
	}
}

func NewChordServer(opts ...ServerOptionFunc) *Server {
	opt := newDefaultServerOption()
	for _, o := range opts {
		o(opt)
	}
	localNode := NewLocalNode(opt.host)
	transport := NewChordApiClient(localNode, opt.timeoutConnNode)
	return &Server{
		process: NewProcess(localNode, transport),
		opt:     opt,
	}
}

func (cs *Server) newGrpcServer() *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	RegisterChordServiceServer(s, cs)
	return s
}

// Run runs chord server.
func (cs *Server) Run(ctx context.Context) {
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", cs.opt.host, ServerPort))
		if err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		grpcServer := cs.newGrpcServer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
	}()
	go func() {
		if err := cs.process.Start(ctx, cs.opt.processOpts...); err != nil {
			log.Fatalf("failed to run chord server. reason: %#v", err)
		}
		<-ctx.Done()
		cs.process.Shutdown()
	}()
}

func (cs *Server) Successor(_ context.Context, _ *empty.Empty) (*Node, error) {
	succ := cs.process.Node.Successor
	if succ == nil {
		return nil, status.Errorf(codes.Internal, "server: internal error occured. successor is not set.")
	}
	return &Node{
		Host: succ.Reference().Host,
	}, nil
}

func (cs *Server) Predecessor(_ context.Context, _ *empty.Empty) (*Node, error) {
	pred := cs.process.Node.Predecessor
	if pred != nil {
		return &Node{
			Host: pred.Reference().Host,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "server: predecessor is not set.")
}

func (cs *Server) FindSuccessorByTable(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessorByTable(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Reference().Host,
	}, nil
}

func (cs *Server) FindSuccessorByList(ctx context.Context, req *FindRequest) (*Node, error) {
	successor, err := cs.process.Node.FindSuccessorByList(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find successor fallback failed. reason = %#v", err)
	}
	return &Node{
		Host: successor.Reference().Host,
	}, nil
}

func (cs *Server) FindClosestPrecedingNode(ctx context.Context, req *FindRequest) (*Node, error) {
	node, err := cs.process.Node.FindClosestPrecedingNode(ctx, req.Id)
	if err == ErrStabilizeNotCompleted {
		return nil, status.Error(codes.NotFound, "Stabilize not completed.")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: find closest preceding node failed. reason = %#v", err)
	}
	return &Node{
		Host: node.Reference().Host,
	}, nil
}

func (cs *Server) Notify(ctx context.Context, req *Node) (*empty.Empty, error) {
	err := cs.process.Node.Notify(ctx, NewRemoteNode(req.Host, cs.process.transport))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "server: notify failed. reason = %#v", err)
	}
	return &empty.Empty{}, nil
}
