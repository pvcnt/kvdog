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
	server *grpc.Server
	logger *log.Logger
}

// NewServer creates a new KV server instance
func NewServer(cfg Config) (*Server, error) {
	log.Printf("Starting kvdog server with grpc_addr=%s", cfg.GRPCAddr)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	kv, err := store.NewBoltStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}

	server := grpc.NewServer()
	svc := &grpcService{
		kv:     kv,
		logger: log.Default(),
	}
	pb.RegisterKVServiceServer(server, svc)

	return &Server{
		server: server,
		logger: log.Default(),
	}, nil
}

func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

func (s *Server) Shutdown() error {
	s.server.GracefulStop()
	return nil
}
