package chord

import (
	"context"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Process struct {
	Node                  *LocalNode
	SuccessorStabilizer   Stabilizer
	FingerTableStabilizer Stabilizer
	isShutdown            bool

	opt *processOption
}

type processOption struct {
	successorStabilizerInterval   time.Duration
	fingerTableStabilizerInterval time.Duration
	timeoutConnNode               time.Duration
	existNode                     *NodeRef
}

type ProcessOption func(option *processOption)

func newDefaultOption() *processOption {
	return &processOption{
		successorStabilizerInterval:   1 * time.Second,
		fingerTableStabilizerInterval: 1 * time.Second,
		timeoutConnNode:               1 * time.Second,
	}
}

func WithSuccessorStabilizeInterval(duration time.Duration) ProcessOption {
	return func(option *processOption) {
		option.successorStabilizerInterval = duration
	}
}

func WithFingerTableStabilizeInterval(duration time.Duration) ProcessOption {
	return func(option *processOption) {
		option.fingerTableStabilizerInterval = duration
	}
}

func WithExistNode(host string, port string) ProcessOption {
	return func(option *processOption) {
		option.existNode = &NodeRef{
			ID:   NewHashID(host),
			Host: host,
			Port: port,
		}
	}
}

func NewProcess(host string, port string, repo NodeRepository) *Process {
	node := NewLocalNode(host, port, repo)
	process := &Process{
		Node: node,
	}
	process.SuccessorStabilizer = SuccessorStabilizer{Node: node}
	process.FingerTableStabilizer = FingerTableStabilizer{Node: node}
	return process
}

func (p *Process) Start(ctx context.Context, opts ...ProcessOption) error {
	p.opt = newDefaultOption()
	for _, opt := range opts {
		opt(p.opt)
	}
	if err := p.Node.Activate(ctx, p.opt.existNode); err != nil {
		return err
	}
	p.scheduleStabilizer(ctx, p.opt.successorStabilizerInterval, p.SuccessorStabilizer)
	p.scheduleStabilizer(ctx, p.opt.fingerTableStabilizerInterval, p.FingerTableStabilizer)
	return nil
}

func (p *Process) Shutdown() {
	p.isShutdown = true
}

func (p *Process) scheduleStabilizer(ctx context.Context, interval time.Duration, stabilizer Stabilizer) {
	if p.isShutdown {
		return
	}
	go func() {
		stabilizer.Stabilize(ctx)
		time.AfterFunc(interval, func() {
			p.scheduleStabilizer(ctx, interval, stabilizer)
		})
	}()
}
