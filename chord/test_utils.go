package chord

import (
	"context"
	"fmt"
	"math/big"
	"sync"
)

func checkStabilize(expectedFingerTable []*Finger, actualFingerTable []*Finger) bool {
	for i, finger := range expectedFingerTable {
		if actualFingerTable[i].Node == nil {
			return false
		}
		if !finger.Node.Reference().ID.Equals(actualFingerTable[i].Node.Reference().ID) {
			return false
		}
	}
	return true
}

func generateExpectedFingerTable(processes []*Process) [][]*Finger {
	tables := make([][]*Finger, len(processes))
	maxNodeID := processes[len(processes)-1].ID
	for i, process := range processes {
		table := NewFingerTable(process.ID)
		for _, finger := range table {
			if finger.ID.GreaterThanEqual(maxNodeID) {
				finger.Node = processes[0].LocalNode
			}
			for _, p := range processes {
				if p.ID.Equals(finger.ID) {
					finger.Node = p.LocalNode
				}
			}
		}
		tables[i] = table
	}
	return tables
}

func generateProcesses(ctx context.Context, processCount int) []*Process {
	var (
		mockTransport = &MockTransport{}
	)
	processes := make([]*Process, processCount)
	nodes := make([]*LocalNode, processCount)
	for i := range processes {
		nodes[i] = NewLocalNode(fmt.Sprintf("gord%d", i+1))
		nodes[i].ID = big.NewInt(int64(i + 1)).Bytes()
		nodes[i].fingerTable = NewFingerTable(nodes[i].ID)
		processes[i] = NewProcess(nodes[i], mockTransport)
	}
	expectedTables := generateExpectedFingerTable(processes)
	for i := range processes {
		if i == 0 {
			processes[i].Start(ctx)
			continue
		}
		processes[i].Start(ctx, WithExistNode(nodes[i-1]))
	}

	wg := sync.WaitGroup{}
	wg.Add(processCount)
	for i, process := range processes {
		expTable := expectedTables[i]
		actTable := process.fingerTable
		go func() {
			for !checkStabilize(expTable, actTable) {
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return processes
}
