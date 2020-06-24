package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
)

type RemoteNode struct {
	*model.NodeRef
	Transport
}

func NewRemoteNode(host string, transport Transport) RingNode {
	return &RemoteNode{
		NodeRef:   model.NewNodeRef(host, ServerPort),
		Transport: transport,
	}
}

func (r *RemoteNode) Reference() *model.NodeRef {
	return r.NodeRef
}

func (r *RemoteNode) GetSuccessor(ctx context.Context) (RingNode, error) {
	return r.SuccessorRPC(ctx, r.NodeRef)
}

func (r *RemoteNode) GetPredecessor(ctx context.Context) (RingNode, error) {
	return r.PredecessorRPC(ctx, r.NodeRef)
}

func (r *RemoteNode) FindSuccessorByList(ctx context.Context, id model.HashID) (RingNode, error) {
	return r.FindSuccessorByListRPC(ctx, r.NodeRef, id)
}

func (r *RemoteNode) FindSuccessorByTable(ctx context.Context, id model.HashID) (RingNode, error) {
	return r.FindSuccessorByTableRPC(ctx, r.NodeRef, id)
}

func (r *RemoteNode) FindClosestPrecedingNode(ctx context.Context, id model.HashID) (RingNode, error) {
	return r.FindClosestPrecedingNodeRPC(ctx, r.NodeRef, id)
}

func (r *RemoteNode) Notify(ctx context.Context, node RingNode) error {
	return r.NotifyRPC(ctx, r.NodeRef, node.Reference())
}
