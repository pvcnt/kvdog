package server

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/pvcnt/kvdog/internal/store"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type Server struct {
	node   *Node
	server *grpc.Server
	logger *log.Logger
}

// NewServer creates a new KV server instance
func NewServer(cfg Config) (*Server, error) {
	log.Printf("Starting kvdog server with node_id=%s, raft_addr=%s, grpc_addr=%s",
		cfg.NodeID, cfg.RaftAddr, cfg.GRPCAddr)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", cfg.NodeID), log.LstdFlags)

	kv, err := store.NewBoltStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}

	node, err := NewNode(cfg, kv, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %v", err)
	}

	server := grpc.NewServer()
	svc := &grpcService{
		n:      node,
		logger: logger,
	}
	pb.RegisterKVServiceServer(server, svc)

	return &Server{
		node:   node,
		server: server,
		logger: logger,
	}, nil
}

func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

func (s *Server) Shutdown() error {
	s.server.GracefulStop()
	if err := s.node.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown raft node: %v", err)
	}
	return nil
}
