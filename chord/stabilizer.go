package chord

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

type Stabilizer interface {
	Stabilize(ctx context.Context)
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
	// Check successors alive
	index := s.Node.FirstAliveSuccessorIndex(ctx)
	if index < 0 {
		log.Errorf("successor stabilizer failed. reason: no successor is available.")
		return
	}
	s.Node.CopySuccessorList(0, s.Node.Successors[index:])

	// Check new successor
	n, err := s.Node.Successors[0].GetPredecessor(ctx)
	if err != nil && err != ErrNotFound {
		log.Errorf("successor stabilizer failed. err = %#v", err)
		return
	}
	if n != nil && n.Reference().ID.Between(s.Node.ID, s.Node.Successors[0].Reference().ID) {
		if err := n.Ping(ctx); err == nil {
			s.Node.CopySuccessorList(1, s.Node.Successors[0:len(s.Node.Successors)-1])
			s.Node.Successors[0] = n
			log.Infof("Host[%s] updated the successor.", s.Node.Host)
		}
	}
	// Notify successor
	err = s.Node.Successors[0].Notify(ctx, s.Node)
	if err != nil {
		log.Errorf("Host[%s] couldn't notify Host[%s]. err = %#v", s.Node.Host, s.Node.Successors[0].Reference().Host, err)
		return
	}
	if s.Node.ID.Equals(s.Node.Successors[0].Reference().ID) {
		return
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
