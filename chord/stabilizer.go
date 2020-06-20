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

func (s SuccessorStabilizer) Stabilize(ctx context.Context) {
	// Check whether there are other nodes between s and the successor
	n, err := s.Node.nodeRepo.PredecessorRPC(ctx, s.Node.Successor)
	if err != nil {
		log.Warnf("Successor Stabilizer failed to get predecessor. err = %#v, successor = %#v", err, s.Node)
		return
	}
	if n != nil && n.ID.Between(s.Node.ID, s.Node.Successor.ID) {
		s.Node.Successor = n
	}
	if err := s.Node.nodeRepo.NotifyRPC(ctx, &s.Node.NodeRef, s.Node.Successor); err != nil {
		log.Warnf("Successor Stabilizer failed to notify. err = %#v, successor = %#v", err, s.Node)
	}
}

type FingerTableStabilizer struct {
	Node *LocalNode
}

func (s FingerTableStabilizer) Stabilize(ctx context.Context) {
	n := rand.Intn(bitSize-2) + 2 // [2,m)
	succ, err := s.Node.FindSuccessor(ctx, s.Node.FingerTable[n].ID)
	if err != nil {
		log.Warnf("FingerTable Stabilizer failed to get successor. err = %#v, id = %#v", err, s.Node.FingerTable[n].ID)
		return
	}
	s.Node.FingerTable[n].Node = succ
}
