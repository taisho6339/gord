package chord

import "math/rand"

type Stabilizer interface {
	Stabilize()
}

type SuccessorStabilizer struct {
	Node *ServerNode
}

func (s SuccessorStabilizer) Stabilize() {
	// Check whether there are other nodes between s and the successor
	n := s.Node.Successor.Predecessor
	if n != nil && n.ID.Between(s.Node.ID, s.Node.Successor.ID) {
		s.Node.Successor = n
	}
	s.Node.Successor.Notify(s.Node)
}

type FingerTableStabilizer struct {
	Node *ServerNode
}

func (s FingerTableStabilizer) Stabilize() {
	n := rand.Intn(bitSize-2) + 2 // [2,m)
	succ := s.Node.FindSuccessorForFingerTable(s.Node.FingerTable[n].ID)
	s.Node.FingerTable[n].Node = succ
}
