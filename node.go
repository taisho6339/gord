package main

import (
	"context"
	"math/rand"
	"time"
)

type ServerNode struct {
	ID          HashID
	Host        string
	FingerTable []*Finger
	Successor   *ServerNode //FIXME: TOBE Successor List
	Predecessor *ServerNode
}

func newServerNode(host string) *ServerNode {
	node := &ServerNode{
		ID:   NewHashID(host),
		Host: host,
	}
	table := make([]*Finger, bitSize)
	for i := range table {
		table[i] = NewFinger(node, i, nil)
	}
	node.FingerTable = table
	return node
}

func CreateChordRing(firstHost string) *ServerNode {
	node := newServerNode(firstHost)
	node.Successor = node
	node.Predecessor = node
	// There is only this node in chord network
	for _, finger := range node.FingerTable {
		finger.Node = node
	}
	return node
}

func JoinNode(newHost string, existNode *ServerNode) *ServerNode {
	node := newServerNode(newHost)
	successor := existNode.FindSuccessor(node.ID)
	node.SetSuccessor(successor)
	return node
}

func (s *ServerNode) StartNode(ctx context.Context) {
	rand.Seed(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				s.Stabilize()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				s.FixFingers()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *ServerNode) SetSuccessor(node *ServerNode) {
	s.Successor = node
	s.FingerTable[0].Node = node
}

// First, search for finger table
// If finger table return nil, search for successor
func (s *ServerNode) FindSuccessor(id HashID) *ServerNode {
	targetNode := *s
	for {
		if id.Between(targetNode.ID, targetNode.Successor.ID.NextID()) {
			break
		}
		finger := targetNode.closestPrecedingFinger(id)
		if finger == nil {
			return s.FallbackFindSuccessor(id)
		}
		targetNode = *finger.Node
	}
	return targetNode.Successor
}

func (s *ServerNode) FallbackFindSuccessor(id HashID) *ServerNode {
	if id.Between(s.ID, s.Successor.ID) {
		return s.Successor
	}
	return s.Successor.FallbackFindSuccessor(id)
}

func (s *ServerNode) closestPrecedingFinger(id HashID) *Finger {
	for i := range s.FingerTable {
		finger := s.FingerTable[len(s.FingerTable)-(i+1)]
		// If the FingerTable has not been updated
		if finger.Node == nil {
			return nil
		}
		if finger.Node.ID.Between(s.ID, id) {
			return finger
		}
	}
	return nil
}

func (s *ServerNode) Stabilize() {
	// Check whether there are other nodes between s and the successor
	n := s.Successor.Predecessor
	if n != nil && (n.ID.Between(s.ID, s.Successor.ID)) {
		s.Successor = n
	}
	s.Successor.Notify(s)
}

func (s *ServerNode) Notify(node *ServerNode) {
	// Fix predecessor if needed
	if s.Predecessor == nil || node.ID.GreaterThan(s.Predecessor.ID) && node.ID.LessThan(s.ID) {
		s.Successor.Predecessor = s
	}
}

func (s *ServerNode) FixFingers() {
	n := rand.Intn(bitSize-2) + 2 // [2,m)
	s.FingerTable[n].Node = s.FindSuccessor(s.FingerTable[n].ID)
}
