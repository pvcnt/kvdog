package server

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pvcnt/kvdog/internal/client"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type grpcService struct {
	pb.UnimplementedKVServiceServer
	n       *Node
	logger  *log.Logger
	clients sync.Map
}

// GetItem retrieves a value for the given key
func (s *grpcService) GetItem(ctx context.Context, req *pb.GetItemRequest) (*pb.GetItemResponse, error) {
	s.logger.Printf("GET %v", req.GetKey())
	value, found := s.n.Get(req.Key)
	return &pb.GetItemResponse{
		Value: value,
		Found: found,
	}, nil
}

// PutItem stores a value for the given key
func (s *grpcService) PutItem(ctx context.Context, req *pb.PutItemRequest) (*pb.PutItemResponse, error) {
	s.logger.Printf("PUT %v", req.GetKey())

	// Forward to the leader node if this node is not the leader.
	if !s.n.IsLeader() {
		c, err := s.getLeaderClient()
		if err != nil {
			return nil, fmt.Errorf("failed to forward request to leader: %v", err)
		}
		rsp, err := c.PutItem(ctx, req)
		if err != nil {
			if isTransportError(err) {
				return nil, fmt.Errorf("failed to forward request to leader: %v", err)
			}
			return nil, err
		}
		return rsp, nil
	}

	// If we are the leader, process the request locally.
	if err := s.n.Put(req.Key, req.Value); err != nil {
		return nil, err
	}
	return &pb.PutItemResponse{}, nil
}

// DeleteItem removes a key from the store
func (s *grpcService) DeleteItem(ctx context.Context, req *pb.DeleteItemRequest) (*pb.DeleteItemResponse, error) {
	s.logger.Printf("DELETE %v", req.GetKey())

	// Forward to the leader node if this node is not the leader.
	if !s.n.IsLeader() {
		c, err := s.getLeaderClient()
		if err != nil {
			return nil, fmt.Errorf("failed to forward request to leader: %v", err)
		}
		rsp, err := c.DeleteItem(ctx, req)
		if err != nil {
			if isTransportError(err) {
				return nil, fmt.Errorf("failed to forward request to leader: %v", err)
			}
			return nil, err
		}
		return rsp, nil
	}

	// If we are the leader, process the request locally.
	if err := s.n.Delete(req.Key); err != nil {
		return nil, err
	}
	return &pb.DeleteItemResponse{}, nil
}

func (s *grpcService) getLeaderClient() (pb.KVServiceClient, error) {
	peer, hasLeader := s.n.Leader()
	if !hasLeader {
		return nil, fmt.Errorf("no leader found")
	}
	if c, ok := s.clients.Load(peer.ID); ok {
		return c.(pb.KVServiceClient), nil
	}
	c, err := client.New(peer.GRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for leader %s: %v", peer.ID, err)
	}
	if v, loaded := s.clients.LoadOrStore(peer.ID, c); loaded {
		_ = c.Close()
		return v.(pb.KVServiceClient), nil
	}
	return c, nil
}

func isTransportError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status error, treat as transport error
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}
