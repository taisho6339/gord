package chord

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Process struct {
	*LocalNode
	SuccessorStabilizer   Stabilizer
	FingerTableStabilizer Stabilizer
	Transport             Transport

	opt        *processOption
	isShutdown bool
	sucChan    chan RingNode
	finChan    chan *Finger
}

type processOption struct {
	successorStabilizerInterval   time.Duration
	fingerTableStabilizerInterval time.Duration
	timeoutConnNode               time.Duration
	existNode                     RingNode
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

func WithExistNode(node RingNode) ProcessOptionFunc {
	return func(option *processOption) {
		option.existNode = node
	}
}

func NewProcess(localNode *LocalNode, transport Transport) *Process {
	process := &Process{
		LocalNode: localNode,
		Transport: transport,
		finChan:   make(chan *Finger),
		sucChan:   make(chan RingNode),
	}
	process.SuccessorStabilizer = NewSuccessorStabilizer(localNode, process.sucChan)
	process.FingerTableStabilizer = NewFingerTableStabilizer(localNode, process.finChan)
	return process
}

func (p *Process) Start(ctx context.Context, opts ...ProcessOptionFunc) error {
	p.opt = newDefaultProcessOption()
	for _, opt := range opts {
		opt(p.opt)
	}
	if p.opt.existNode != nil && p.opt.existNode.Reference().Host == p.Host {
		log.Fatalf("exist node must be different from local node.")
	}
	if err := p.activate(ctx, p.opt.existNode); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case suc := <-p.sucChan:
				p.Successor = suc
			case finger := <-p.finChan:
				p.FingerTable[finger.Index] = finger
			}
		}
	}()
	p.scheduleStabilizer(ctx, p.opt.successorStabilizerInterval, p.SuccessorStabilizer)
	p.scheduleStabilizer(ctx, p.opt.fingerTableStabilizerInterval, p.FingerTableStabilizer)
	return nil
}

func (p *Process) activate(ctx context.Context, existNode RingNode) error {
	// This localnode is first node for chord ring.
	if existNode == nil {
		p.Successor = p.LocalNode
		p.Predecessor = p.LocalNode
		// There is only this node in chord network
		for _, finger := range p.LocalNode.FingerTable {
			finger.Node = p.LocalNode
		}
		return nil
	}

	successor, err := existNode.FindSuccessorByTable(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("find successor rpc failed. err = %#v", err)
	}
	p.Successor = successor
	p.FingerTable[0].Node = successor
	if err := p.Successor.Notify(ctx, p.LocalNode); err != nil {
		return fmt.Errorf("notify rpc failed. err = %#v", err)
	}
	return nil
}

func (p *Process) Shutdown() {
	p.isShutdown = true
	p.Transport.Shutdown()
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
