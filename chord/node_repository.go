package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
)

type NodeRepository interface {
	SuccessorRPC(ctx context.Context, ref *model.NodeRef) (*model.NodeRef, error)
	PredecessorRPC(ctx context.Context, ref *model.NodeRef) (*model.NodeRef, error)

	FindSuccessorRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error)
	FindSuccessorFallbackRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error)
	FindClosestPrecedingNodeRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error)
	NotifyRPC(ctx context.Context, fromRef *model.NodeRef, toRef *model.NodeRef) error

	Shutdown()
}
