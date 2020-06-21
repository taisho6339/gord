package chord

import (
	"context"
)

type NodeRepository interface {
	SuccessorRPC(ctx context.Context, ref *NodeRef) (*NodeRef, error)
	PredecessorRPC(ctx context.Context, ref *NodeRef) (*NodeRef, error)

	FindSuccessorRPC(ctx context.Context, ref *NodeRef, id HashID) (*NodeRef, error)
	FindSuccessorFallbackRPC(ctx context.Context, ref *NodeRef, id HashID) (*NodeRef, error)
	FindClosestPrecedingNodeRPC(ctx context.Context, ref *NodeRef, id HashID) (*NodeRef, error)
	NotifyRPC(ctx context.Context, fromRef *NodeRef, toRef *NodeRef) error
}
