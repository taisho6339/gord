package gordserver

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"github.com/taisho6339/gord/chord"
	"google.golang.org/grpc"
	"io"
	"time"
)

type ChordApiClient struct {
	timeout time.Duration
}

func NewChordApiClient(timeout time.Duration) chord.NodeRepository {
	return &ChordApiClient{
		timeout: timeout,
	}
}

// TODO: Enable mTLS
func (c *ChordApiClient) newGrpcConn(address string) (io.Closer, ChordInternalServiceClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, NewChordInternalServiceClient(conn), nil
}

func (c *ChordApiClient) SuccessorRPC(ctx context.Context, ref *chord.NodeRef) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.Successor(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("client: successor rpc failed. reason = %#v", err)
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) PredecessorRPC(ctx context.Context, ref *chord.NodeRef) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.Predecessor(ctx, &empty.Empty{})
	if err != nil {
		log.Warnf("client: predecessor rpc failed. reason = %#v", err)
		return nil, chord.ErrNotFound
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) FindSuccessorRPC(ctx context.Context, ref *chord.NodeRef, id chord.HashID) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.FindSuccessor(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("client: find successor rpc failed. reason = %#v", err)
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) FindSuccessorFallbackRPC(ctx context.Context, ref *chord.NodeRef, id chord.HashID) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.FindSuccessorFallback(ctx, &FindRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("client: find successor fallback rpc failed. reason = %#v", err)
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) FindClosestPrecedingNodeRPC(ctx context.Context, ref *chord.NodeRef, id chord.HashID) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.FindClosestPrecedingNode(ctx, &FindRequest{Id: id})
	if err != nil {
		log.Warnf("client: find closest preceding rpc failed. reason = %#v", err)
		return nil, err
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) NotifyRPC(ctx context.Context, fromRef *chord.NodeRef, toRef *chord.NodeRef) error {
	conn, client, err := c.newGrpcConn(toRef.Address())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
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
