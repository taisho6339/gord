package chord

import (
	"context"
	"github.com/taisho6339/gord/model"
	"testing"
	"time"
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
		node3Name = "node3"
		node1     = NewLocalNode(node1Name)
		node2     = NewLocalNode(node2Name)
		node3     = NewLocalNode(node3Name)
		process1  = NewProcess(node1, mockTransport)
		process2  = NewProcess(node2, mockTransport)
		process3  = NewProcess(node3, mockTransport)
	)
	ctx := context.Background()
	if err := process1.Start(ctx); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}
	if err := process2.Start(ctx, WithExistNode(node1)); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}
	if err := process3.Start(ctx, WithExistNode(node2)); err != nil {
		t.Fatalf("start failed. err = %#v", err)
	}

	done := make(chan struct{}, 1)
	timeout := make(chan struct{}, 1)
	go func() {
		for {
			if process1.Successor == nil || process1.Predecessor == nil ||
				process2.Successor == nil || process2.Predecessor == nil ||
				process3.Successor == nil || process3.Predecessor == nil {
				continue
			}
			if process1.Successor.Reference().ID.Equals(process2.ID) &&
				process2.Successor.Reference().ID.Equals(process3.ID) &&
				process3.Successor.Reference().ID.Equals(process1.ID) &&
				process1.Predecessor.Reference().ID.Equals(process3.ID) &&
				process2.Predecessor.Reference().ID.Equals(process1.ID) &&
				process3.Predecessor.Reference().ID.Equals(process2.ID) {
				break
			}
		}
		done <- struct{}{}
	}()
	go func() {
		time.AfterFunc(time.Second*10, func() {
			timeout <- struct{}{}
		})
	}()
	select {
	case <-done:
		break
	case <-timeout:
		t.Fatal("test failed. stabilize timeout.")
	}
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
			findKey:        node1Name,
			expectedHost:   node1Name,
			callingProcess: process3,
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
		{
			findKey:        node2Name,
			expectedHost:   node2Name,
			callingProcess: process3,
		},
		{
			findKey:        node3Name,
			expectedHost:   node3Name,
			callingProcess: process1,
		},
		{
			findKey:        node3Name,
			expectedHost:   node3Name,
			callingProcess: process2,
		},
		{
			findKey:        node3Name,
			expectedHost:   node3Name,
			callingProcess: process3,
		},
	}
	for _, testcase := range testcases {
		t.Logf("[INFO] Start test. find key = %s, callingProcess = %s, expectedHost = %s", testcase.findKey, testcase.callingProcess.Host, testcase.expectedHost)
		succ, err := testcase.callingProcess.FindSuccessorByTable(ctx, model.NewHashID(testcase.findKey))
		if err != nil {
			t.Fatalf("find successor by table failed. err = %#v", err)
		}
		if succ.Reference().Host != testcase.expectedHost {
			t.Fatalf("expected host is %s, but %s", testcase.expectedHost, succ.Reference().Host)
		}
		succ, err = testcase.callingProcess.FindSuccessorByList(ctx, model.NewHashID(testcase.findKey))
		if err != nil {
			t.Fatalf("find successor by list failed. err = %#v", err)
		}
		if succ.Reference().Host != testcase.expectedHost {
			t.Fatalf("expected host is %s, but %s", testcase.expectedHost, succ.Reference().Host)
		}
	}
}
