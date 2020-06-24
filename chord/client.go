package chord

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/model"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type ApiClient struct {
	hostNode *LocalNode
	timeout  time.Duration
	connPool map[string]*grpc.ClientConn
	poolLock sync.Mutex
	opts     grpc.CallOption
}

func NewChordApiClient(host *LocalNode, timeout time.Duration) Transport {
	return &ApiClient{
		hostNode: host,
		timeout:  timeout,
		connPool: map[string]*grpc.ClientConn{},
	}
}

// TODO: Enable mTLS
// TODO: Add conn pool capacity limit for file descriptors.
func (c *ApiClient) getGrpcConn(address string) (ChordServiceClient, error) {
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	conn, ok := c.connPool[address]
	if ok {
		return NewChordServiceClient(conn), nil
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	c.connPool[address] = conn
	return NewChordServiceClient(conn), nil
}

func (c *ApiClient) createRingNodeFrom(node *Node) RingNode {
	if c.hostNode.Host == node.Host {
		return c.hostNode
	}
	return NewRemoteNode(node.Host, c)
}

func (c *ApiClient) SuccessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error) {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	node, err := client.Successor(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("successor rpc failed. reason = %#v", err)
	}
	return c.createRingNodeFrom(node), nil
}

func (c *ApiClient) PredecessorRPC(ctx context.Context, to *model.NodeRef) (RingNode, error) {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	node, err := client.Predecessor(ctx, &empty.Empty{})
	if err != nil {
		log.Warnf("predecessor rpc failed. reason = %#v", err)
		return nil, ErrNotFound
	}
	return c.createRingNodeFrom(node), nil
}

func (c *ApiClient) FindSuccessorByTableRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	node, err := client.FindSuccessorByTable(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("find successor rpc failed. reason = %#v", err)
	}
	return c.createRingNodeFrom(node), nil
}

func (c *ApiClient) FindSuccessorByListRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	node, err := client.FindSuccessorByList(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("find successor fallback rpc failed. reason = %#v", err)
	}
	return c.createRingNodeFrom(node), nil
}

func (c *ApiClient) FindClosestPrecedingNodeRPC(ctx context.Context, to *model.NodeRef, id model.HashID) (RingNode, error) {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	node, err := client.FindClosestPrecedingNode(ctx, &FindRequest{Id: id})
	if err != nil {
		log.Warnf("find closest preceding rpc failed. reason = %#v", err)
		return nil, err
	}
	return c.createRingNodeFrom(node), nil
}

func (c *ApiClient) NotifyRPC(ctx context.Context, to *model.NodeRef, node *model.NodeRef) error {
	client, err := c.getGrpcConn(to.Address())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	_, err = client.Notify(ctx, &Node{
		Host: node.Host,
	})
	if err != nil {
		return fmt.Errorf("notify rpc failed. reason = %#v", err)
	}
	return nil
}

func (c *ApiClient) Shutdown() {
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	for _, conn := range c.connPool {
		conn.Close()
	}
}
