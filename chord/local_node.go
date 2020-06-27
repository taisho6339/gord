package chord

import (
	"context"
	"errors"
	"fmt"
	"github.com/taisho6339/gord/model"
)

type LocalNode struct {
	*model.NodeRef
	FingerTable []*Finger
	Successors  []RingNode
	Predecessor RingNode
}

func NewLocalNode(host string) *LocalNode {
	id := model.NewHashID(host)
	return &LocalNode{
		NodeRef:     model.NewNodeRef(host),
		FingerTable: NewFingerTable(id),
	}
}

func (l *LocalNode) initSuccessors(succ RingNode) {
	successors := make([]RingNode, l.ID.Size()/2)
	successors[0] = succ
	l.Successors = successors
	l.FingerTable[0].Node = succ
}

func (l *LocalNode) CreateRing() {
	l.initSuccessors(l)
	l.Predecessor = l
	for _, finger := range l.FingerTable {
		finger.Node = l
	}
}

func (l *LocalNode) JoinRing(ctx context.Context, existNode RingNode) error {
	successor, err := existNode.FindSuccessorByTable(ctx, l.ID)
	if err != nil {
		return fmt.Errorf("find successor failed. err = %#v", err)
	}
	l.initSuccessors(successor)
	err = l.Successors[0].Notify(ctx, l)
	if err != nil {
		return fmt.Errorf("notify failed. err = %#v", err)
	}
	successors, err := l.Successors[0].GetSuccessors(ctx)
	if err != nil {
		return fmt.Errorf("get successors failed. err = %#v", err)
	}
	l.UpdateSuccessorList(successors[0 : len(successors)-1])
	return nil
}

func (l *LocalNode) UpdateSuccessorList(successors []RingNode) {
	var newSuccessors []RingNode
	for _, succ := range successors {
		if succ == nil {
			break
		}
		if succ.Reference().ID.Equals(l.ID) {
			break
		}
		newSuccessors = append(newSuccessors, succ)
	}
	copy(l.Successors[1:], newSuccessors[:])
}

func (l *LocalNode) Reference() *model.NodeRef {
	return l.NodeRef
}

func (l *LocalNode) GetSuccessors(_ context.Context) ([]RingNode, error) {
	return l.Successors, nil
}

func (l *LocalNode) GetPredecessor(_ context.Context) (RingNode, error) {
	return l.Predecessor, nil
}

func (l *LocalNode) FindSuccessorByList(ctx context.Context, id model.HashID) (RingNode, error) {
	if l.ID.Equals(l.Successors[0].Reference().ID) {
		return l, nil
	}
	if id.Equals(l.ID) {
		return l, nil
	}
	if id.Between(l.ID, l.Successors[0].Reference().ID) {
		return l.Successors[0], nil
	}
	for i, successor := range l.Successors {
		if successor == nil {
			continue
		}
		ret, err := successor.FindSuccessorByList(ctx, id)
		if err == nil {
			l.dequeueFailureSuccessor(i)
			return ret, nil
		}
	}
	return nil, errors.New("there is no alive successor")
}

func (l *LocalNode) dequeueFailureSuccessor(aliveIndex int) {
	newSuccessors := make([]RingNode, len(l.Successors))
	copy(newSuccessors, l.Successors[aliveIndex:])
	l.Successors = newSuccessors
}

func (l *LocalNode) FindSuccessorByTable(ctx context.Context, id model.HashID) (RingNode, error) {
	node, err := l.findPredecessor(ctx, id)
	if err != nil {
		return l.FindSuccessorByList(ctx, id)
	}
	successor, err := node.GetSuccessors(ctx)
	if err != nil {
		return nil, err
	}
	return successor[0], nil
}

func (l *LocalNode) findPredecessor(ctx context.Context, id model.HashID) (RingNode, error) {
	var (
		targetNode RingNode = l
	)
	for {
		successor, err := targetNode.GetSuccessors(ctx)
		if err != nil {
			return nil, err
		}
		if successor[0] == nil {
			return nil, ErrNotFound
		}
		if targetNode.Reference().ID.Equals(successor[0].Reference().ID) {
			return targetNode, nil
		}
		if id.Between(targetNode.Reference().ID, successor[0].Reference().ID.NextID()) {
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
