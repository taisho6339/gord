package chord

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/model"
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

	transport  Transport
	isShutdown bool
	opt        *processOption
}

type processOption struct {
	successorStabilizerInterval   time.Duration
	fingerTableStabilizerInterval time.Duration
	timeoutConnNode               time.Duration
	existNode                     *model.NodeRef
}

type ProcessOptionFunc func(option *processOption)

func newDefaultProcessOption() *processOption {
	return &processOption{
		successorStabilizerInterval:   5 * time.Second,
		fingerTableStabilizerInterval: 500 * time.Millisecond,
		timeoutConnNode:               1 * time.Second,
	}
}

func WithSuccessorStabilizeInterval(duration time.Duration) ProcessOptionFunc {
	return func(option *processOption) {
		option.successorStabilizerInterval = duration
	}
}

func WithFingerTableStabilizeInterval(duration time.Duration) ProcessOptionFunc {
	return func(option *processOption) {
		option.fingerTableStabilizerInterval = duration
	}
}

func WithExistNode(host string) ProcessOptionFunc {
	return func(option *processOption) {
		option.existNode = model.NewNodeRef(host, ServerPort)
	}
}

func NewProcess(localNode *LocalNode, transport Transport) *Process {
	process := &Process{
		Node:      localNode,
		transport: transport,
	}
	process.SuccessorStabilizer = SuccessorStabilizer{Node: localNode}
	process.FingerTableStabilizer = FingerTableStabilizer{Node: localNode}
	return process
}

func (p *Process) Start(ctx context.Context, opts ...ProcessOptionFunc) error {
	p.opt = newDefaultProcessOption()
	for _, opt := range opts {
		opt(p.opt)
	}
	if p.opt.existNode != nil && p.opt.existNode.Address() == p.Node.Address() {
		log.Fatalf("exist node must be different from local node.")
	}
	if err := p.activate(ctx, p.opt.existNode); err != nil {
		return err
	}
	p.scheduleStabilizer(ctx, p.opt.successorStabilizerInterval, p.SuccessorStabilizer)
	p.scheduleStabilizer(ctx, p.opt.fingerTableStabilizerInterval, p.FingerTableStabilizer)
	return nil
}

func (p *Process) activate(ctx context.Context, existNode *model.NodeRef) error {
	// This localnode is first node for chord ring.
	if existNode == nil {
		p.Node.Successor = p.Node
		p.Node.Predecessor = p.Node
		// There is only this node in chord network
		for _, finger := range p.Node.FingerTable {
			finger.Node = p.Node
		}
		return nil
	}

	node := NewRemoteNode(p.opt.existNode.Host, p.transport)
	successor, err := node.FindSuccessorByTable(ctx, p.Node.ID)
	if err != nil {
		return fmt.Errorf("find successor rpc failed. err = %#v", err)
	}
	p.Node.Successor = successor
	p.Node.FingerTable[0].Node = successor
	if err := p.Node.Successor.Notify(ctx, p.Node); err != nil {
		return fmt.Errorf("notify rpc failed. err = %#v", err)
	}
	return nil
}

func (p *Process) Shutdown() {
	p.isShutdown = true
	p.transport.Shutdown()
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
