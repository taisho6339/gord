package chord

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/model"
	"math/rand"
)

type Stabilizer interface {
	Stabilize(ctx context.Context)
}

type AliveStabilizer struct {
	Node *LocalNode
}

func NewAliveStabilizer(node *LocalNode) AliveStabilizer {
	return AliveStabilizer{
		Node: node,
	}
}

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

type SuccessorStabilizer struct {
	Node *LocalNode
}

func NewSuccessorStabilizer(node *LocalNode) SuccessorStabilizer {
	return SuccessorStabilizer{
		Node: node,
	}
}

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
	s.Node.successors.join(1, successors)
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
	n := rand.Intn(model.BitSize-2) + 2 // [2,m)
	succ, err := s.Node.FindSuccessorByTable(ctx, s.Node.fingerTable[n].ID)
	if err != nil {
		log.Warnf("stabilizer: Host[%s] couldn't find successor. err = %#v, finger id = %x", s.Node.Host, err, s.Node.fingerTable[n].ID)
		return
	}
	s.Node.fingerTable[n].Node = succ

	// Try to update as many finger entries as possible
	for i := n + 1; i < len(s.Node.fingerTable); i++ {
		finger := s.Node.fingerTable[i]
		if finger.ID.LessThanEqual(succ.Reference().ID) {
			s.Node.fingerTable[i].Node = succ
			continue
		}
		break
	}
}
