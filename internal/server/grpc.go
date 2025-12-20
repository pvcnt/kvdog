package server

import (
	"context"
	"log"

	"github.com/pvcnt/kvdog/internal/store"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type grpcService struct {
	pb.UnimplementedKVServiceServer
	kv     store.Store
	logger *log.Logger
}

// GetItem retrieves a value for the given key
func (s *grpcService) GetItem(ctx context.Context, req *pb.GetItemRequest) (*pb.GetItemResponse, error) {
	s.logger.Printf("GET %v", req.GetKey())
	value, found := s.kv.Get(req.Key)
	return &pb.GetItemResponse{
		Value: value,
		Found: found,
	}, nil
}

// PutItem stores a value for the given key
func (s *grpcService) PutItem(ctx context.Context, req *pb.PutItemRequest) (*pb.PutItemResponse, error) {
	s.logger.Printf("PUT %v", req.GetKey())
	if err := s.kv.Set(req.Key, req.Value); err != nil {
		return nil, err
	}
	return &pb.PutItemResponse{}, nil
}

// DeleteItem removes a key from the store
func (s *grpcService) DeleteItem(ctx context.Context, req *pb.DeleteItemRequest) (*pb.DeleteItemResponse, error) {
	s.logger.Printf("DELETE %v", req.GetKey())
	if err := s.kv.Delete(req.Key); err != nil {
		return nil, err
	}
	return &pb.DeleteItemResponse{}, nil
}
