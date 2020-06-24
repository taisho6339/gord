package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
)

type RingNode interface {
	Reference() *model.NodeRef
	GetSuccessor(ctx context.Context) (RingNode, error)
	GetPredecessor(ctx context.Context) (RingNode, error)
	FindSuccessorByTable(ctx context.Context, id model.HashID) (RingNode, error)
	FindSuccessorByList(ctx context.Context, id model.HashID) (RingNode, error)
	FindClosestPrecedingNode(ctx context.Context, id model.HashID) (RingNode, error)
	Notify(ctx context.Context, node RingNode) error
}

type Transport interface {
	SuccessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error)
	PredecessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error)
	FindSuccessorByTableRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error)
	FindSuccessorByListRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error)
	FindClosestPrecedingNodeRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error)
	NotifyRPC(ctx context.Context, to *model.NodeRef, node *model.NodeRef) error
	Shutdown()
}
