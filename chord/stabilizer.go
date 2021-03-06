package chord

import (
	"context"
	log "github.com/sirupsen/logrus"
)

// Stabilizer is a process that runs asynchronously in a single goroutine
type Stabilizer interface {
	Stabilize(ctx context.Context)
}

// AliveStabilizer checks successor status.
// If this stabilizer detects successors dead, remove them from a successor list of a local node.
type AliveStabilizer struct {
	Node *LocalNode
}

// NewAliveStabilizer creates an alive stabilizer
func NewAliveStabilizer(node *LocalNode) AliveStabilizer {
	return AliveStabilizer{
		Node: node,
	}
}

// Stabilize is implemented for Stabilizer interface.
func (a AliveStabilizer) Stabilize(ctx context.Context) {
	aliveNodes := emptyNodes(cap(a.Node.successors.nodes))
	for _, suc := range a.Node.successors.nodes {
		if err := suc.Ping(ctx); err == nil {
			aliveNodes = append(aliveNodes, suc)
			continue
		}
		log.Warnf("Host:[%s] is dead.", suc.Reference().Host)
	}
	if len(aliveNodes) < len(a.Node.successors.nodes) {
		a.Node.JoinSuccessors(0, aliveNodes)
	}
}

// SuccessorStabilizer checks new successors.
// If this stabilizer finds new successor, adds a new one to a successor list of a local node.
// In addition, this notify a successor to check its predecessor.
type SuccessorStabilizer struct {
	Node *LocalNode
}

// NewSuccessorStabilizer creates a successor stabilizer.
func NewSuccessorStabilizer(node *LocalNode) SuccessorStabilizer {
	return SuccessorStabilizer{
		Node: node,
	}
}

// Stabilize is implemented for Stabilizer interface.
func (s SuccessorStabilizer) Stabilize(ctx context.Context) {
	suc, err := s.Node.successors.head()
	if err != nil {
		log.Errorf("no successor is alive. err = %#v", err)
		return
	}
	// Check new successor
	n, err := suc.GetPredecessor(ctx)
	if err != nil && err != ErrNotFound {
		log.Errorf("successor stabilizer failed. err = %#v", err)
		return
	}
	if n != nil && n.Reference().ID.Between(s.Node.ID, suc.Reference().ID) {
		if err := n.Ping(ctx); err == nil {
			log.Infof("Host[%s] updated its successor.", s.Node.Host)
			s.Node.PutSuccessor(n)
		}
	}
	// Notify successor
	err = suc.Notify(ctx, s.Node)
	if err != nil {
		log.Errorf("Host[%s] couldn't notify Host[%s]. err = %#v", s.Node.Host, suc.Reference().Host, err)
		return
	}
	if s.Node.ID.Equals(suc.Reference().ID) {
		return
	}
	// Update successor list
	successors, err := suc.GetSuccessors(ctx)
	if err != nil {
		log.Warnf("Host[%s] couldn't get successors from Host[%s]. err = %#v", s.Node.Host, suc.Reference().Host, err)
		return
	}
	s.Node.lock.Lock()
	defer s.Node.lock.Unlock()
	s.Node.successors.join(1, successors)
}

// FingerTableStabilizer maintains a finger table of a local node.
type FingerTableStabilizer struct {
	Node                *LocalNode
	lastStabilizedIndex int
}

// NewFingerTableStabilizer creates a finger table stabilizer.
func NewFingerTableStabilizer(node *LocalNode) *FingerTableStabilizer {
	return &FingerTableStabilizer{
		Node:                node,
		lastStabilizedIndex: -1,
	}
}

// Stabilize is implemented for Stabilizer interface.
func (s *FingerTableStabilizer) Stabilize(ctx context.Context) {
	index := (s.lastStabilizedIndex + 1) % cap(s.Node.fingerTable)
	succ, err := s.Node.FindSuccessorByTable(ctx, s.Node.fingerTable[index].ID)
	if err != nil {
		return
	}
	s.Node.fingerTable[index].Node = succ
	s.lastStabilizedIndex = index
	// Try to update as many finger entries as possible
	for i := index + 1; i < cap(s.Node.fingerTable); i++ {
		finger := s.Node.fingerTable[i]
		if finger.ID.LessThanEqual(succ.Reference().ID) {
			s.Node.fingerTable[i].Node = succ
			s.lastStabilizedIndex = i
			continue
		}
		s.Node.fingerTable[i].Node = succ
		break
	}
}
