package chord

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type NodeRef struct {
	ID   HashID
	Host string
	Port string
}

func NewNodeRef(host string, port string) *NodeRef {
	return &NodeRef{
		ID:   NewHashID(fmt.Sprintf("%s:%s", host, port)),
		Host: host,
		Port: port,
	}
}

func (n *NodeRef) Address() string {
	return fmt.Sprintf("%s:%s", n.Host, n.Port)
}

type LocalNode struct {
	*NodeRef
	FingerTable []*Finger
	Successor   *NodeRef
	Predecessor *NodeRef

	nodeRepo   NodeRepository
	notifyLock sync.RWMutex
	succLock   sync.RWMutex
	fingerLock sync.RWMutex
}

func NewLocalNode(host string, port string, nodeRepository NodeRepository) *LocalNode {
	n := &LocalNode{
		NodeRef:  NewNodeRef(host, port),
		nodeRepo: nodeRepository,
	}
	n.FingerTable = n.newFingerTable()
	return n
}

func (l *LocalNode) newFingerTable() []*Finger {
	table := make([]*Finger, bitSize)
	for i := range table {
		table[i] = NewFinger(l.ID, i, nil)
	}
	return table
}

func (l *LocalNode) Activate(ctx context.Context, existNode *NodeRef) error {
	// This localnode is first node for chord ring.
	if existNode == nil {
		l.Successor = l.NodeRef
		l.Predecessor = l.NodeRef
		// There is only this node in chord network
		for _, finger := range l.FingerTable {
			finger.Node = l.NodeRef
		}
		return nil
	}
	successor, err := l.nodeRepo.FindSuccessorRPC(ctx, existNode, l.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("activate: find successor rpc failed. err = %#v", err))
	}
	l.Successor = successor
	l.FingerTable[0].Node = successor
	if err := l.nodeRepo.NotifyRPC(ctx, l.Successor, l.NodeRef); err != nil {
		return errors.New(fmt.Sprintf("activate: notify rpc failed. err = %#v", err))
	}
	return nil
}

func (l *LocalNode) FindSuccessor(ctx context.Context, id HashID) (*NodeRef, error) {
	node, err := l.findPredecessor(ctx, id)
	if err != nil {
		return l.FindSuccessorFallback(ctx, id)
	}
	successor, err := l.nodeRepo.SuccessorRPC(ctx, node)
	if err != nil {
		return nil, err
	}
	return successor, nil
}

func (l *LocalNode) FindSuccessorFallback(ctx context.Context, id HashID) (*NodeRef, error) {
	if l.ID.Equals(l.Successor.ID) {
		return l.NodeRef, nil
	}
	if id.Equals(l.ID) {
		return l.NodeRef, nil
	}
	if id.Between(l.ID, l.Successor.ID) {
		return l.Successor, nil
	}
	return l.nodeRepo.FindSuccessorFallbackRPC(ctx, l.Successor, id)
}

func (l *LocalNode) findPredecessor(ctx context.Context, id HashID) (*NodeRef, error) {
	var (
		targetNode = l.NodeRef
	)
	for {
		successor, err := l.nodeRepo.SuccessorRPC(ctx, targetNode)
		if err != nil {
			return nil, err
		}
		if targetNode.ID.Equals(successor.ID) {
			return targetNode, nil
		}
		if id.Between(targetNode.ID, successor.ID.NextID()) {
			break
		}
		node, err := l.nodeRepo.FindClosestPrecedingNodeRPC(ctx, targetNode, id)
		if err != nil {
			return nil, ErrNotFound
		}
		targetNode = node
	}
	return targetNode, nil
}

func (l *LocalNode) FindClosestPrecedingNode(id HashID) (*NodeRef, error) {
	for i := range l.FingerTable {
		finger := l.FingerTable[len(l.FingerTable)-(i+1)]
		// If the FingerTable has not been updated
		if finger.Node == nil {
			return nil, ErrStabilizeNotCompleted
		}
		if finger.Node.ID.Between(l.ID, id) {
			return finger.Node, nil
		}
	}
	return l.NodeRef, nil
}

func (l *LocalNode) Notify(node *NodeRef) error {
	l.notifyLock.Lock()
	defer l.notifyLock.Unlock()
	if l.Predecessor == nil || node.ID.Between(l.Predecessor.ID, l.ID) {
		l.Predecessor = node
	}
	return nil
}
