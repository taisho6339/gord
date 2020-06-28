package chord

import (
	"context"
	"github.com/taisho6339/gord/chord/test"
	"github.com/taisho6339/gord/model"
	"testing"
	"time"
)

var mockTransport = &MockTransport{}

func prepareProcesses(t *testing.T, ctx context.Context, node1Name, node2Name, node3Name string) (*Process, *Process, *Process) {
	var (
		node1    = NewLocalNode(node1Name)
		node2    = NewLocalNode(node2Name)
		node3    = NewLocalNode(node3Name)
		process1 = NewProcess(node1, mockTransport)
		process2 = NewProcess(node2, mockTransport)
		process3 = NewProcess(node3, mockTransport)
	)
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
			if process1.Successors == nil || process1.Predecessor == nil ||
				process2.Successors == nil || process2.Predecessor == nil ||
				process3.Successors == nil || process3.Predecessor == nil {
				continue
			}
			// Check successor list
			if len(process1.Successors) <= 1 || len(process2.Successors) <= 1 || len(process3.Successors) <= 1 {
				continue
			}
			if process1.Successors[0].Reference().ID.Equals(process2.ID) &&
				process1.Successors[1].Reference().ID.Equals(process3.ID) &&
				process2.Successors[0].Reference().ID.Equals(process3.ID) &&
				process2.Successors[1].Reference().ID.Equals(process1.ID) &&
				process3.Successors[0].Reference().ID.Equals(process1.ID) &&
				process3.Successors[1].Reference().ID.Equals(process2.ID) &&
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
	return process1, process2, process3
}

func TestProcess_SingleNode(t *testing.T) {
	defer test.PanicFail(t)
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

func TestProcess_MultiNodes(t *testing.T) {
	defer test.PanicFail(t)
	var (
		node1Name = "node1"
		node2Name = "node2"
		node3Name = "node3"
	)
	ctx := context.Background()
	process1, process2, process3 := prepareProcesses(t, ctx, node1Name, node2Name, node3Name)
	defer process1.Shutdown()
	defer process2.Shutdown()
	defer process3.Shutdown()

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
		t.Logf("[INFO] Start test. process is %s. find key = %s, callingProcess = %s, expectedHost = %s", testcase.callingProcess.Host, testcase.findKey, testcase.callingProcess.Host, testcase.expectedHost)
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

func TestProcess_Stabilize_SuccessorList(t *testing.T) {
	defer test.PanicFail(t)
	var (
		node1Name = "node1"
		node2Name = "node2"
		node3Name = "node3"
	)
	ctx := context.Background()
	process1, process2, process3 := prepareProcesses(t, ctx, node1Name, node2Name, node3Name)
	defer process1.Shutdown()
	defer process2.Shutdown()
	defer process3.Shutdown()
	testcases := []struct {
		targetNode            RingNode
		expectedSuccessorList []RingNode
	}{
		{
			targetNode: process1.LocalNode,
			expectedSuccessorList: []RingNode{
				process2.LocalNode,
				process3.LocalNode,
				process1.LocalNode,
			},
		},
		{
			targetNode: process2.LocalNode,
			expectedSuccessorList: []RingNode{
				process3.LocalNode,
				process1.LocalNode,
			},
		},
		{
			targetNode: process3.LocalNode,
			expectedSuccessorList: []RingNode{
				process1.LocalNode,
				process2.LocalNode,
			},
		},
	}
	for _, testcase := range testcases {
		t.Logf("[TESTCASE] %s", testcase.targetNode.Reference().Host)
		ctx := context.Background()
		successors, err := testcase.targetNode.GetSuccessors(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i, suc := range testcase.expectedSuccessorList {
			if !successors[i].Reference().ID.Equals(suc.Reference().ID) {
				t.Fatalf("expected successor host is %s, but successor host = %s", suc.Reference().Host, successors[i].Reference().Host)
			}
		}
	}
}

//func TestProcess_Node_Failure(t *testing.T) {
//	defer test.PanicFail(t)
//	var (
//		node1Name = "node1"
//		node2Name = "node2"
//		node3Name = "node3"
//	)
//	ctx := context.Background()
//	process1, process2, process3 := prepareProcesses(t, ctx, node1Name, node2Name, node3Name)
//
//}
