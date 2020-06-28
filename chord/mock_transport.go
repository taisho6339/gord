package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
)

type MockTransport struct{}

func (m *MockTransport) PingRPC(ctx context.Context, to *model.NodeRef) error {
	return nil
}

func (m *MockTransport) SuccessorsRPC(ctx context.Context, to *model.NodeRef) ([]RingNode, error) {
	return nil, nil
}
func (m *MockTransport) PredecessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error) {
	return nil, nil
}
func (m *MockTransport) FindSuccessorByTableRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	return nil, nil
}
func (m *MockTransport) FindSuccessorByListRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	return nil, nil
}
func (m *MockTransport) FindClosestPrecedingNodeRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	return nil, nil
}
func (m *MockTransport) NotifyRPC(ctx context.Context, to *model.NodeRef, node *model.NodeRef) error {
	return nil
}
func (m *MockTransport) Shutdown() {
}
