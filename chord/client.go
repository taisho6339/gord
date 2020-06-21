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
	timeout  time.Duration
	connPool map[string]*grpc.ClientConn
	poolLock sync.Mutex
}

func NewChordApiClient(timeout time.Duration) NodeRepository {
	return &ApiClient{
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

func (c *ApiClient) SuccessorRPC(ctx context.Context, ref *model.NodeRef) (*model.NodeRef, error) {
	client, err := c.getGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	node, err := client.Successor(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("client: successor rpc failed. reason = %#v", err)
	}
	return model.NewNodeRef(node.Host, node.Port), nil
}

func (c *ApiClient) PredecessorRPC(ctx context.Context, ref *model.NodeRef) (*model.NodeRef, error) {
	client, err := c.getGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	node, err := client.Predecessor(ctx, &empty.Empty{})
	if err != nil {
		log.Warnf("client: predecessor rpc failed. reason = %#v", err)
		return nil, ErrNotFound
	}
	return model.NewNodeRef(node.Host, node.Port), nil
}

func (c *ApiClient) FindSuccessorRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error) {
	client, err := c.getGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	node, err := client.FindSuccessor(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("client: find successor rpc failed. reason = %#v", err)
	}
	return model.NewNodeRef(node.Host, node.Port), nil
}

func (c *ApiClient) FindSuccessorFallbackRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error) {
	client, err := c.getGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	node, err := client.FindSuccessorFallback(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("client: find successor fallback rpc failed. reason = %#v", err)
	}
	return model.NewNodeRef(node.Host, node.Port), nil
}

func (c *ApiClient) FindClosestPrecedingNodeRPC(ctx context.Context, ref *model.NodeRef, id model.HashID) (*model.NodeRef, error) {
	client, err := c.getGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	node, err := client.FindClosestPrecedingNode(ctx, &FindRequest{Id: id})
	if err != nil {
		log.Warnf("client: find closest preceding rpc failed. reason = %#v", err)
		return nil, err
	}
	return model.NewNodeRef(node.Host, node.Port), nil
}

func (c *ApiClient) NotifyRPC(ctx context.Context, fromRef *model.NodeRef, toRef *model.NodeRef) error {
	client, err := c.getGrpcConn(toRef.Address())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	_, err = client.Notify(ctx, &Node{
		Host: fromRef.Host,
		Port: fromRef.Port,
	})
	if err != nil {
		return fmt.Errorf("client: notify rpc failed. reason = %#v", err)
	}
	return nil
}
