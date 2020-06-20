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

	opt *processOption
}

type processOption struct {
	stabilizerInterval time.Duration
	timeoutConnNode    time.Duration
	existNode          *NodeRef
}

type ProcessOption func(option *processOption)

func newDefaultOption() *processOption {
	return &processOption{
		stabilizerInterval: 500 * time.Millisecond,
		timeoutConnNode:    1 * time.Second,
	}
}

func StabilizeInterval(duration time.Duration) ProcessOption {
	return func(option *processOption) {
		option.stabilizerInterval = duration
	}
}

func ExistNode(host string, port string) ProcessOption {
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

func (p *Process) StartProcess(ctx context.Context, opts ...ProcessOption) error {
	p.opt = newDefaultOption()
	for _, opt := range opts {
		opt(p.opt)
	}
	if err := p.Node.Activate(ctx, p.opt.existNode); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(p.opt.stabilizerInterval)
		for {
			select {
			case <-ticker.C:
				p.SuccessorStabilizer.Stabilize(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(p.opt.stabilizerInterval)
		for {
			select {
			case <-ticker.C:
				p.FingerTableStabilizer.Stabilize(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	return nil
}
