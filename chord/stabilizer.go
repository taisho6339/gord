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
	Node       *LocalNode
	notifyChan chan RingNode
}

func NewSuccessorStabilizer(node *LocalNode, notifyChan chan RingNode) SuccessorStabilizer {
	return SuccessorStabilizer{
		Node:       node,
		notifyChan: notifyChan,
	}
}

func (s SuccessorStabilizer) Stabilize(ctx context.Context) {
	// Check whether there are other nodes between s and the successor
	n, err := s.Node.Successor.GetPredecessor(ctx)
	if err != nil && err != ErrNotFound {
		log.Warnf("successor stabilizer failed. err = %#v", err)
		return
	}
	if n != nil && n.Reference().ID.Between(s.Node.ID, s.Node.Successor.Reference().ID) {
		s.notifyChan <- n
		log.Infof("stabilizer: Host[%s] updated the successor.", s.Node.Host)
	}
	if err := s.Node.Successor.Notify(ctx, s.Node); err != nil {
		log.Warnf("stabilizer: Host[%s] couldn't notify Host[%s]. err = %#v", s.Node.Host, s.Node.Successor.Reference().Host, err)
	}
}

type FingerTableStabilizer struct {
	Node       *LocalNode
	notifyChan chan *Finger
}

func NewFingerTableStabilizer(node *LocalNode, notifyChan chan *Finger) FingerTableStabilizer {
	return FingerTableStabilizer{
		Node:       node,
		notifyChan: notifyChan,
	}
}

func (s FingerTableStabilizer) Stabilize(ctx context.Context) {
	i := rand.Intn(s.Node.ID.Size()-2) + 2 // [2,m)
	succ, err := s.Node.FindSuccessorByTable(ctx, s.Node.FingerTable[i].ID)
	if err != nil {
		log.Warnf("stabilizer: Host[%s] couldn't find successor. err = %#v, finger id = %x", s.Node.Host, err, s.Node.FingerTable[i].ID)
		return
	}
	s.notifyChan <- NewFinger(s.Node.ID, i, succ)
}
