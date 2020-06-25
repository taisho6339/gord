package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
	"testing"
)

type MockTransport struct{}

func (m *MockTransport) SuccessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error) {
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

var mockTransport = &MockTransport{}

func Test_SingleNode(t *testing.T) {
	hostName := "single"
	node := NewLocalNode(hostName)
	process := NewProcess(node, mockTransport)
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}

	ctx := context.Background()
	succ, err := process.FindSuccessorByTable(ctx, model.NewHashID(hostName))
	if err != nil {
		t.Fatalf("find successor by table failed. err = %#v", err)
	}
	if succ.Reference().Host != hostName {
		t.Fatalf("expected host is %s, but %s", hostName, succ.Reference().Host)
	}
	succ, err = process.FindSuccessorByList(ctx, model.NewHashID(hostName))
	if err != nil {
		t.Fatalf("find successor by list failed. err = %#v", err)
	}
	if succ.Reference().Host != hostName {
		t.Fatalf("expected host is %s, but %s", hostName, succ.Reference().Host)
	}
}

func Test_MultiNodes(t *testing.T) {
	var (
		node1Name = "node1"
		node2Name = "node2"
		node1     = NewLocalNode(node1Name)
		node2     = NewLocalNode(node2Name)
		process1  = NewProcess(node1, mockTransport)
		process2  = NewProcess(node2, mockTransport)
	)
	ctx := context.Background()
	if err := process1.Start(ctx); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}
	if err := process2.Start(ctx, WithExistNode(node1)); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}
	process1.SuccessorStabilizer.Stabilize(ctx)

	testcases := []struct {
		findKey        string
		expectedHost   string
		callingProcess *Process
	}{
		{
			findKey:        node1Name,
			expectedHost:   node1Name,
			callingProcess: process1,
		},
		{
			findKey:        node1Name,
			expectedHost:   node1Name,
			callingProcess: process2,
		},
		{
			findKey:        node2Name,
			expectedHost:   node2Name,
			callingProcess: process1,
		},
		{
			findKey:        node2Name,
			expectedHost:   node2Name,
			callingProcess: process2,
		},
	}
	for _, testcase := range testcases {
		t.Logf("[INFO] Start test. find key = %s, callingProcess = %s, expectedHost = %s", testcase.findKey, testcase.callingProcess.Host, testcase.expectedHost)
		succ, err := testcase.callingProcess.FindSuccessorByList(ctx, model.NewHashID(testcase.findKey))
		if err != nil {
			t.Fatalf("find successor by list failed. err = %#v", err)
		}
		if succ.Reference().Host != testcase.expectedHost {
			t.Fatalf("expected host is %s, but %s", testcase.expectedHost, succ.Reference().Host)
		}
	}
}
