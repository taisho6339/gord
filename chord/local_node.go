package chord

import (
	"context"
	"fmt"
	"github.com/taisho6339/gord/model"
)

type LocalNode struct {
	*model.NodeRef
	FingerTable []*Finger
	Successors  []RingNode
	Predecessor RingNode
	isShutdown  bool
}

func NewLocalNode(host string) *LocalNode {
	id := model.NewHashID(host)
	return &LocalNode{
		NodeRef:     model.NewNodeRef(host),
		FingerTable: NewFingerTable(id),
	}
}

func (l *LocalNode) initSuccessors(succ RingNode) {
	l.Successors = make([]RingNode, l.ID.Size()/2)
	l.Successors[0] = succ
	l.FingerTable[0].Node = succ
}

func (l *LocalNode) Shutdown() {
	l.isShutdown = true
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
	l.CopySuccessorList(1, successors[0:len(successors)-1])
	return nil
}

func (l *LocalNode) CopySuccessorList(offset int, successors []RingNode) {
	var filteredSuccessors []RingNode
	maskSuccessors := make([]RingNode, len(l.Successors))
	for _, succ := range successors {
		if succ == nil {
			continue
		}
		filteredSuccessors = append(filteredSuccessors, succ)
	}
	copy(maskSuccessors, filteredSuccessors)
	copy(l.Successors[offset:], maskSuccessors[0:len(maskSuccessors)-offset])
}

func (l *LocalNode) Ping(_ context.Context) error {
	if l.isShutdown {
		return ErrNodeUnavailable
	}
	return nil
}

func (l *LocalNode) Reference() *model.NodeRef {
	return l.NodeRef
}

func (l *LocalNode) GetSuccessors(_ context.Context) ([]RingNode, error) {
	if l.isShutdown {
		return nil, ErrNodeUnavailable
	}
	return l.Successors, nil
}

func (l *LocalNode) GetPredecessor(_ context.Context) (RingNode, error) {
	if l.isShutdown {
		return nil, ErrNodeUnavailable
	}
	return l.Predecessor, nil
}

func (l *LocalNode) FindSuccessorByList(ctx context.Context, id model.HashID) (RingNode, error) {
	if l.isShutdown {
		return nil, ErrNodeUnavailable
	}
	if l.ID.Equals(l.Successors[0].Reference().ID) {
		return l, nil
	}
	if id.Equals(l.ID) {
		return l, nil
	}
	var aliveSuccessor RingNode = nil
	for _, s := range l.Successors {
		if err := s.Ping(ctx); err == nil {
			aliveSuccessor = s
			break
		}
	}
	if aliveSuccessor == nil {
		return nil, ErrNoSuccessorAlive
	}
	if id.Between(l.ID, aliveSuccessor.Reference().ID) {
		return aliveSuccessor, nil
	}
	return aliveSuccessor.FindSuccessorByList(ctx, id)
}

func (l *LocalNode) FindSuccessorByTable(ctx context.Context, id model.HashID) (RingNode, error) {
	if l.isShutdown {
		return nil, ErrNodeUnavailable
	}
	node, err := l.findPredecessor(ctx, id)
	if err != nil {
		return l.FindSuccessorByList(ctx, id)
	}
	successors, err := node.GetSuccessors(ctx)
	if err != nil {
		return nil, err
	}
	for _, successor := range successors {
		if err := successor.Ping(ctx); err == nil {
			return successor, nil
		}
	}
	return nil, ErrNoSuccessorAlive
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
	if l.isShutdown {
		return nil, ErrNodeUnavailable
	}
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
	if l.isShutdown {
		return ErrNodeUnavailable
	}
	if l.Predecessor == nil || node.Reference().ID.Between(l.Predecessor.Reference().ID, l.ID) {
		l.Predecessor = node
	}
	return nil
}
