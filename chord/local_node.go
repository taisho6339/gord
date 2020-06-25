package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
)

type LocalNode struct {
	*model.NodeRef
	FingerTable []*Finger
	Successor   RingNode
	Predecessor RingNode
}

func NewLocalNode(host string) *LocalNode {
	id := model.NewHashID(host)
	return &LocalNode{
		NodeRef:     model.NewNodeRef(host),
		FingerTable: NewFingerTable(id),
	}
}

func (l *LocalNode) Reference() *model.NodeRef {
	return l.NodeRef
}

func (l *LocalNode) GetSuccessor(_ context.Context) (RingNode, error) {
	return l.Successor, nil
}

func (l *LocalNode) GetPredecessor(_ context.Context) (RingNode, error) {
	return l.Predecessor, nil
}

func (l *LocalNode) FindSuccessorByList(ctx context.Context, id model.HashID) (RingNode, error) {
	if l.ID.Equals(l.Successor.Reference().ID) {
		return l, nil
	}
	if id.Equals(l.ID) {
		return l, nil
	}
	if id.Between(l.ID, l.Successor.Reference().ID) {
		return l.Successor, nil
	}
	return l.Successor.FindSuccessorByList(ctx, id)
}

func (l *LocalNode) FindSuccessorByTable(ctx context.Context, id model.HashID) (RingNode, error) {
	node, err := l.findPredecessor(ctx, id)
	if err != nil {
		return l.FindSuccessorByList(ctx, id)
	}
	successor, err := node.GetSuccessor(ctx)
	if err != nil {
		return nil, err
	}
	return successor, nil
}

func (l *LocalNode) findPredecessor(ctx context.Context, id model.HashID) (RingNode, error) {
	var (
		targetNode RingNode = l
	)
	for {
		successor, err := targetNode.GetSuccessor(ctx)
		if err != nil {
			return nil, err
		}
		if targetNode.Reference().ID.Equals(successor.Reference().ID) {
			return targetNode, nil
		}
		if id.Between(targetNode.Reference().ID, successor.Reference().ID.NextID()) {
			break
		}
		node, err := targetNode.FindClosestPrecedingNode(ctx, id)
		if err != nil {
			return nil, ErrNotFound
		}
		targetNode = node
	}
	return targetNode, nil
}

func (l *LocalNode) FindClosestPrecedingNode(_ context.Context, id model.HashID) (RingNode, error) {
	for i := range l.FingerTable {
		finger := l.FingerTable[len(l.FingerTable)-(i+1)]
		// If the FingerTable has not been updated
		if finger.Node == nil {
			return nil, ErrStabilizeNotCompleted
		}
		if finger.Node.Reference().ID.Between(l.ID, id) {
			return finger.Node, nil
		}
	}
	return l, nil
}

func (l *LocalNode) Notify(_ context.Context, node RingNode) error {
	if l.Predecessor == nil || node.Reference().ID.Between(l.Predecessor.Reference().ID, l.ID) {
		l.Predecessor = node
	}
	return nil
}
