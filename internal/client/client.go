package client

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Client wraps the gRPC client for the kvdog service
type Client struct {
	pb.KVServiceClient
	conn *grpc.ClientConn
}

// New creates a new Client connected to the specified server address
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return &Client{
		KVServiceClient: pb.NewKVServiceClient(conn),
		conn:            conn,
	}, nil
}

// Close closes the underlying gRPC connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
