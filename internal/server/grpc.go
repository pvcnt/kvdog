package server

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pvcnt/kvdog/internal/client"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type grpcService struct {
	pb.UnimplementedKVServiceServer
	nodes    map[int]*Node // Map of shard ID to Node
	metadata Metadata
	cfg      Config
	logger   *log.Logger
	clients  sync.Map
}

// GetItem retrieves a value for the given key
func (s *grpcService) GetItem(ctx context.Context, req *pb.GetItemRequest) (*pb.GetItemResponse, error) {
	s.logger.Printf("GET %v", req.GetKey())

	// Find which shard holds this key
	shard, err := s.metadata.FindShard(req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to find shard for key: %v", err)
	}

	// Check if we have a local replica of this shard
	if node, ok := s.nodes[shard.ID]; ok {
		// We have a local replica, read from it
		value, found := node.Get(req.Key)
		return &pb.GetItemResponse{
			Value: value,
			Found: found,
		}, nil
	}

	// Forward to a random replica of this shard
	replica := shard.Replicas[rand.Intn(len(shard.Replicas))]
	c, err := s.getClientForPeer(replica.PeerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for peer %s: %v", replica.PeerID, err)
	}

	rsp, err := c.GetItem(ctx, req)
	if err != nil {
		if isTransportError(err) {
			return nil, fmt.Errorf("failed to forward request to peer %s: %v", replica.PeerID, err)
		}
		return nil, err
	}
	return rsp, nil
}

// PutItem stores a value for the given key
func (s *grpcService) PutItem(ctx context.Context, req *pb.PutItemRequest) (*pb.PutItemResponse, error) {
	s.logger.Printf("PUT %v", req.GetKey())

	// Find which shard holds this key
	shard, err := s.metadata.FindShard(req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to find shard for key: %v", err)
	}

	// Check if we have a local replica of this shard
	if node, ok := s.nodes[shard.ID]; ok {
		// We have a local replica
		if !node.IsLeader() {
			// Forward to the leader of this shard
			leader, hasLeader := node.Leader()
			if !hasLeader {
				return nil, fmt.Errorf("no leader found for shard %d", shard.ID)
			}

			// Extract the peer ID from the leader ID (format: "peerID-shard-X")
			peerID := s.extractPeerID(leader.ID)
			c, err := s.getClientForPeer(peerID)
			if err != nil {
				return nil, fmt.Errorf("failed to get client for leader %s: %v", peerID, err)
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

		// We are the leader, process locally
		if err := node.Put(req.Key, req.Value); err != nil {
			return nil, err
		}
		return &pb.PutItemResponse{}, nil
	}

	// Forward to any replica of this shard (which will then forward to leader if needed)
	replica := shard.Replicas[0]
	c, err := s.getClientForPeer(replica.PeerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for peer %s: %v", replica.PeerID, err)
	}

	rsp, err := c.PutItem(ctx, req)
	if err != nil {
		if isTransportError(err) {
			return nil, fmt.Errorf("failed to forward request to peer %s: %v", replica.PeerID, err)
		}
		return nil, err
	}
	return rsp, nil
}

// DeleteItem removes a key from the store
func (s *grpcService) DeleteItem(ctx context.Context, req *pb.DeleteItemRequest) (*pb.DeleteItemResponse, error) {
	s.logger.Printf("DELETE %v", req.GetKey())

	// Find which shard holds this key
	shard, err := s.metadata.FindShard(req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to find shard for key: %v", err)
	}

	// Check if we have a local replica of this shard
	if node, ok := s.nodes[shard.ID]; ok {
		// We have a local replica
		if !node.IsLeader() {
			// Forward to the leader of this shard
			leader, hasLeader := node.Leader()
			if !hasLeader {
				return nil, fmt.Errorf("no leader found for shard %d", shard.ID)
			}

			// Extract the peer ID from the leader ID (format: "peerID-shard-X")
			peerID := s.extractPeerID(leader.ID)
			c, err := s.getClientForPeer(peerID)
			if err != nil {
				return nil, fmt.Errorf("failed to get client for leader %s: %v", peerID, err)
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

		// We are the leader, process locally
		if err := node.Delete(req.Key); err != nil {
			return nil, err
		}
		return &pb.DeleteItemResponse{}, nil
	}

	// Forward to any replica of this shard (which will then forward to leader if needed)
	replica := shard.Replicas[0]
	c, err := s.getClientForPeer(replica.PeerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for peer %s: %v", replica.PeerID, err)
	}

	rsp, err := c.DeleteItem(ctx, req)
	if err != nil {
		if isTransportError(err) {
			return nil, fmt.Errorf("failed to forward request to peer %s: %v", replica.PeerID, err)
		}
		return nil, err
	}
	return rsp, nil
}

// extractPeerID extracts the peer ID from a node ID (format: "peerID-shard-X")
func (s *grpcService) extractPeerID(nodeID string) string {
	// Format is "peerID-shard-X", we need to extract "peerID"
	for i := len(nodeID) - 1; i >= 0; i-- {
		if nodeID[i] == '-' {
			for j := i - 1; j >= 0; j-- {
				if nodeID[j] == '-' {
					return nodeID[:j]
				}
			}
		}
	}
	return nodeID
}

// getClientForPeer returns a gRPC client for the given peer
func (s *grpcService) getClientForPeer(peerID string) (pb.KVServiceClient, error) {
	// Find the peer's gRPC address from config
	var grpcAddr string
	for _, peer := range s.cfg.Peers {
		if peer.ID == peerID {
			grpcAddr = peer.GRPCAddr
			break
		}
	}
	if grpcAddr == "" {
		return nil, fmt.Errorf("peer %s not found in config", peerID)
	}

	// Check if we already have a client for this peer
	if c, ok := s.clients.Load(peerID); ok {
		return c.(pb.KVServiceClient), nil
	}

	// Create a new client
	c, err := client.New(grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for peer %s: %v", peerID, err)
	}

	// Store the client, or use existing one if another goroutine created it
	if v, loaded := s.clients.LoadOrStore(peerID, c); loaded {
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
