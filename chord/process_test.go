package chord

import (
	"context"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, process1.Start(ctx))
	assert.NoError(t, process2.Start(ctx, WithExistNode(node1)))
	assert.NoError(t, process3.Start(ctx, WithExistNode(node2)))
	test.WaitCheckFuncWithTimeout(t, func() bool {
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
	}, 10*time.Second)
	return process1, process2, process3
}

func TestProcess_SingleNode(t *testing.T) {
	defer test.PanicFail(t)
	ctx := context.Background()
	hostName := "single"
	node := NewLocalNode(hostName)
	process := NewProcess(node, mockTransport)
	assert.NoError(t, process.Start(context.Background()))

	succ, err := process.FindSuccessorByTable(ctx, model.NewHashID(hostName))
	assert.Nil(t, err)
	assert.Equal(t, hostName, succ.Reference().Host)

	succ, err = process.FindSuccessorByList(ctx, model.NewHashID(hostName))
	assert.Nil(t, err)
	assert.Equal(t, hostName, succ.Reference().Host)
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
		assert.Nil(t, err)
		assert.Equal(t, testcase.expectedHost, succ.Reference().Host)

		succ, err = testcase.callingProcess.FindSuccessorByList(ctx, model.NewHashID(testcase.findingKey))
		assert.Nil(t, err)
		assert.Equal(t, testcase.expectedHost, succ.Reference().Host)
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
		ctx := context.Background()
		successors, err := testcase.targetNode.GetSuccessors(ctx)
		assert.Nil(t, err)
		for i, suc := range testcase.expectedSuccessorList {
			assert.Equal(t, suc.Reference().ID, successors[i].Reference().ID)
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
	test.WaitCheckFuncWithTimeout(t, func() bool {
		return len(process2.successors.nodes) == 2 && len(process3.successors.nodes) == 2
	}, 10*time.Second)

	assert.Equal(t, 2, len(process2.successors.nodes))
	assert.Equal(t, 2, len(process3.successors.nodes))
	for _, s := range process2.successors.nodes {
		assert.NotEqual(t, process1.ID, s.Reference().ID)
	}

	process2.Shutdown()
	test.WaitCheckFuncWithTimeout(t, func() bool {
		return len(process3.successors.nodes) == 1
	}, 10*time.Second)

	suc, err := process3.successors.head()
	assert.Nil(t, err)
	assert.Equal(t, process3.ID, suc.Reference().ID)
}
