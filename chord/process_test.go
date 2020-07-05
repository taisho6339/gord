package chord

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/taisho6339/gord/chord/test"
	"github.com/taisho6339/gord/model"
	"math/big"
	"testing"
	"time"
)

var mockTransport = &MockTransport{}

func prepareProcesses(t *testing.T, ctx context.Context, processCount int) []*Process {
	processes := make([]*Process, processCount)
	nodes := make([]*LocalNode, processCount)
	for i := range processes {
		nodes[i] = NewLocalNode(fmt.Sprintf("gord%d", i+1))
		nodes[i].ID = big.NewInt(int64(i + 1)).Bytes()
		processes[i] = NewProcess(nodes[i], mockTransport)
	}
	for i := range processes {
		if i == 0 {
			assert.NoError(t, processes[i].Start(ctx))
			continue
		}
		assert.NoError(t, processes[i].Start(ctx, WithExistNode(nodes[i-1])))
	}
	test.WaitCheckFuncWithTimeout(t, func() bool {
		for i, process := range processes {
			if process == nil {
				return false
			}
			if len(process.successors.nodes) < processCount {
				return false
			}
			for j, successor := range process.successors.nodes {
				index := ((i + j) + 1) % len(process.successors.nodes)
				node := nodes[index]
				if !node.ID.Equals(successor.Reference().ID) {
					return false
				}
			}
			successor, _ := process.successors.head()
			predecessor, _ := successor.GetPredecessor(ctx)
			if predecessor == nil {
				return false
			}
			if !predecessor.Reference().ID.Equals(process.ID) {
				return false
			}
		}
		return true
	}, 10*time.Second)
	return processes
}

func TestProcess_SingleNode(t *testing.T) {
	assert.NotPanics(t, func() {
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
	})
}

func TestProcess_MultiNodes(t *testing.T) {
	ctx := context.Background()
	processes := prepareProcesses(t, ctx, 3)
	process1, process2, process3 := processes[0], processes[1], processes[2]
	defer process1.Shutdown()
	defer process2.Shutdown()
	defer process3.Shutdown()
	var (
		node1Name = "gord1"
		node2Name = "gord2"
		node3Name = "gord3"
	)
	testcases := []struct {
		findingID      model.HashID
		expectedHost   string
		callingProcess *Process
	}{
		{
			findingID:      big.NewInt(1).Bytes(),
			expectedHost:   node1Name,
			callingProcess: process1,
		},
		{
			findingID:      big.NewInt(1).Bytes(),
			expectedHost:   node1Name,
			callingProcess: process2,
		},
		{
			findingID:      big.NewInt(1).Bytes(),
			expectedHost:   node1Name,
			callingProcess: process3,
		},
		{
			findingID:      big.NewInt(2).Bytes(),
			expectedHost:   node2Name,
			callingProcess: process1,
		},
		{
			findingID:      big.NewInt(2).Bytes(),
			expectedHost:   node2Name,
			callingProcess: process2,
		},
		{
			findingID:      big.NewInt(2).Bytes(),
			expectedHost:   node2Name,
			callingProcess: process3,
		},
		{
			findingID:      big.NewInt(3).Bytes(),
			expectedHost:   node3Name,
			callingProcess: process1,
		},
		{
			findingID:      big.NewInt(3).Bytes(),
			expectedHost:   node3Name,
			callingProcess: process2,
		},
		{
			findingID:      big.NewInt(3).Bytes(),
			expectedHost:   node3Name,
			callingProcess: process3,
		},
	}
	for _, testcase := range testcases {
		assert.NotPanics(t, func() {
			t.Logf("[CASE] finding: %x, expected: %s, call node: %s", testcase.findingID, testcase.expectedHost, testcase.callingProcess.Host)
			succ, err := testcase.callingProcess.FindSuccessorByTable(ctx, testcase.findingID)
			assert.Nil(t, err)
			assert.Equal(t, testcase.expectedHost, succ.Reference().Host)

			succ, err = testcase.callingProcess.FindSuccessorByList(ctx, testcase.findingID)
			assert.Nil(t, err)
			assert.Equal(t, testcase.expectedHost, succ.Reference().Host)
		})
	}
}

func TestProcess_Stabilize_SuccessorList(t *testing.T) {
	ctx := context.Background()
	processes := prepareProcesses(t, ctx, 3)
	process1, process2, process3 := processes[0], processes[1], processes[2]
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
		assert.NotPanics(t, func() {
			ctx := context.Background()
			successors, err := testcase.targetNode.GetSuccessors(ctx)
			assert.Nil(t, err)
			for i, suc := range testcase.expectedSuccessorList {
				assert.Equal(t, suc.Reference().ID, successors[i].Reference().ID)
			}
		})
	}
}

func TestProcess_Node_Failure(t *testing.T) {
	ctx := context.Background()
	assert.NotPanics(t, func() {
		processes := prepareProcesses(t, ctx, 3)
		process1, process2, process3 := processes[0], processes[1], processes[2]
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
	})
}
