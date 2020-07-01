package chord

import (
	"context"
	"github.com/taisho6339/gord/chord/test"
	"github.com/taisho6339/gord/model"
	"testing"
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
	test.WaitForFunc(t, func() bool {
		if process1.successors == nil || process1.predecessor == nil ||
			process2.successors == nil || process2.predecessor == nil ||
			process3.successors == nil || process3.predecessor == nil {
			return false
		}
		if len(process1.successors.nodes) < 3 || len(process2.successors.nodes) < 3 || len(process3.successors.nodes) < 3 {
			return false
		}
		if process1.successors.nodes[0].Reference().ID.Equals(process2.ID) &&
			process1.successors.nodes[1].Reference().ID.Equals(process3.ID) &&
			process2.successors.nodes[0].Reference().ID.Equals(process3.ID) &&
			process2.successors.nodes[1].Reference().ID.Equals(process1.ID) &&
			process3.successors.nodes[0].Reference().ID.Equals(process1.ID) &&
			process3.successors.nodes[1].Reference().ID.Equals(process2.ID) &&
			process1.predecessor.Reference().ID.Equals(process3.ID) &&
			process2.predecessor.Reference().ID.Equals(process1.ID) &&
			process3.predecessor.Reference().ID.Equals(process2.ID) {
			return true
		}
		return false
	})
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
		findingKey     string
		expectedHost   string
		callingProcess *Process
	}{
		{
			findingKey:     node1Name,
			expectedHost:   node1Name,
			callingProcess: process1,
		},
		{
			findingKey:     node1Name,
			expectedHost:   node1Name,
			callingProcess: process2,
		},
		{
			findingKey:     node1Name,
			expectedHost:   node1Name,
			callingProcess: process3,
		},
		{
			findingKey:     node2Name,
			expectedHost:   node2Name,
			callingProcess: process1,
		},
		{
			findingKey:     node2Name,
			expectedHost:   node2Name,
			callingProcess: process2,
		},
		{
			findingKey:     node2Name,
			expectedHost:   node2Name,
			callingProcess: process3,
		},
		{
			findingKey:     node3Name,
			expectedHost:   node3Name,
			callingProcess: process1,
		},
		{
			findingKey:     node3Name,
			expectedHost:   node3Name,
			callingProcess: process2,
		},
		{
			findingKey:     node3Name,
			expectedHost:   node3Name,
			callingProcess: process3,
		},
	}
	for _, testcase := range testcases {
		t.Logf("[INFO] Start test. process is %s. find key = %s, callingProcess = %s, expectedHost = %s", testcase.callingProcess.Host, testcase.findingKey, testcase.callingProcess.Host, testcase.expectedHost)
		succ, err := testcase.callingProcess.FindSuccessorByTable(ctx, model.NewHashID(testcase.findingKey))
		if err != nil {
			t.Fatalf("find successor by table failed. err = %#v", err)
		}
		if succ.Reference().Host != testcase.expectedHost {
			t.Fatalf("expected host is %s, but %s", testcase.expectedHost, succ.Reference().Host)
		}
		succ, err = testcase.callingProcess.FindSuccessorByList(ctx, model.NewHashID(testcase.findingKey))
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

func TestProcess_Node_Failure(t *testing.T) {
	defer test.PanicFail(t)
	var (
		node1Name = "node1"
		node2Name = "node2"
		node3Name = "node3"
	)
	ctx := context.Background()
	process1, process2, process3 := prepareProcesses(t, ctx, node1Name, node2Name, node3Name)
	process1.Shutdown()
	test.WaitForFunc(t, func() bool {
		return len(process2.successors.nodes) == 2 && len(process3.successors.nodes) == 2
	})
	if len(process2.successors.nodes) != 2 {
		t.Fatalf("expected %d, but actual %d", 2, len(process2.successors.nodes))
	}
	if len(process3.successors.nodes) != 2 {
		t.Fatalf("expected %d, but actual %d", 2, len(process3.successors.nodes))
	}
	for _, s := range process2.successors.nodes {
		if s.Reference().ID.Equals(process1.ID) {
			t.Fatalf("process1 is dead, but referenced")
		}
	}

	process2.Shutdown()
	test.WaitForFunc(t, func() bool {
		return len(process3.successors.nodes) == 1
	})
	suc, err := process3.successors.head()
	if err != nil {
		t.Fatalf("err = %#v", err)
	}
	if !suc.Reference().ID.Equals(process3.ID) {
		t.Fatalf("only node3 is alive, but refers other nodes")
	}
}
