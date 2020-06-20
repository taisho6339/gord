package gordserver

import (
	"context"
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

func (c *ChordApiClient) newGrpcConn(address string) (io.Closer, ChordServiceClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, NewChordServiceClient(conn), nil
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
		log.Warnf("Failed to get successor from remote node. err = %v", err)
		return nil, err
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
		log.Warnf("Failed to get predecessor from remote node. err = %v", err)
		return nil, err
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
		log.Warnf("Failed to find successor from remote node. err = %v", err)
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
		log.Warnf("Failed to find successor from remote node. err = %v", err)
	}
	return chord.NewNodeRef(node.Host, node.Port), nil
}

func (c *ChordApiClient) FindPredecessorRPC(ctx context.Context, ref *chord.NodeRef, id chord.HashID) (*chord.NodeRef, error) {
	conn, client, err := c.newGrpcConn(ref.Address())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer conn.Close()
	defer cancel()

	node, err := client.FindPredecessor(ctx, &FindRequest{Id: id})
	if err != nil {
		log.Warnf("Failed to find predecessor from remote node. err = %v", err)
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

	node, err := client.FindPredecessor(ctx, &FindRequest{Id: id})
	if err != nil {
		log.Warnf("Failed to find closest preceding node from remote node. err = %v", err)
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
		log.Warnf("Failed to notify node from remote node. err = %v", err)
	}
	return nil
}
