package chord

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

type Stabilizer interface {
	Stabilize(ctx context.Context)
}

type SuccessorAliveChecker struct {
	Node *LocalNode
}

func NewSuccessorAliveChecker(node *LocalNode) SuccessorAliveChecker {
	return SuccessorAliveChecker{
		Node: node,
	}
}

func (s SuccessorAliveChecker) Stabilize(ctx context.Context) {
	successors := make([]RingNode, len(s.Node.Successors))
	for i, successor := range s.Node.Successors {
		if successor == nil {
			continue
		}
		if err := successor.Ping(ctx); err == nil {
			successors[i] = successor
		}
	}
	s.Node.CopySuccessorList(0, successors)
}

type SuccessorStabilizer struct {
	Node *LocalNode
}

func NewSuccessorStabilizer(node *LocalNode) SuccessorStabilizer {
	return SuccessorStabilizer{
		Node: node,
	}
}

func (s SuccessorStabilizer) Stabilize(ctx context.Context) {
	n, err := s.Node.Successors[0].GetPredecessor(ctx)
	if err != nil && err != ErrNotFound {
		log.Warnf("successor stabilizer failed. err = %#v", err)
		return
	}
	// Check whether there are other nodes between s and the successor
	if n != nil && n.Reference().ID.Between(s.Node.ID, s.Node.Successors[0].Reference().ID) {
		copy(s.Node.Successors[1:len(s.Node.Successors)], s.Node.Successors[0:len(s.Node.Successors)-1])
		s.Node.Successors[0] = n
		log.Infof("Host[%s] updated the successor.", s.Node.Host)
	}
	err = s.Node.Successors[0].Notify(ctx, s.Node)
	if err != nil {
		log.Warnf("Host[%s] couldn't notify Host[%s]. err = %#v", s.Node.Host, s.Node.Successors[0].Reference().Host, err)
	}
	// Update successor list
	successors, err := s.Node.Successors[0].GetSuccessors(ctx)
	if err != nil {
		log.Warnf("Host[%s] couldn't get successors from Host[%s]. err = %#v", s.Node.Host, s.Node.Successors[0].Reference().Host, err)
		return
	}
	s.Node.CopySuccessorList(1, successors[0:len(successors)-1])
}

type FingerTableStabilizer struct {
	Node *LocalNode
}

func NewFingerTableStabilizer(node *LocalNode) FingerTableStabilizer {
	return FingerTableStabilizer{
		Node: node,
	}
}

func (s FingerTableStabilizer) Stabilize(ctx context.Context) {
	n := rand.Intn(s.Node.ID.Size()-2) + 2 // [2,m)
	succ, err := s.Node.FindSuccessorByTable(ctx, s.Node.FingerTable[n].ID)
	if err != nil {
		log.Warnf("stabilizer: Host[%s] couldn't find successor. err = %#v, finger id = %x", s.Node.Host, err, s.Node.FingerTable[n].ID)
		return
	}
	s.Node.FingerTable[n].Node = succ

	// Try to update as many finger entries as possible
	for i := n + 1; i < len(s.Node.FingerTable); i++ {
		finger := s.Node.FingerTable[i]
		if finger.ID.LessThanEqual(succ.Reference().ID) {
			s.Node.FingerTable[i].Node = succ
			continue
		}
		break
	}
}
