package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/pvcnt/kvdog/internal/store"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type Server struct {
	nodes    map[int]*Node // Map of shard ID to Node
	metadata Metadata
	server   *grpc.Server
	logger   *log.Logger
}

// NewServer creates a new KV server instance
func NewServer(cfg Config, metadata Metadata) (*Server, error) {
	log.Printf("Starting kvdog server with node_id=%s, raft_addr=%s, grpc_addr=%s",
		cfg.NodeID, cfg.RaftAddr, cfg.GRPCAddr)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", cfg.NodeID), log.LstdFlags)

	// Create a Raft node for each shard that has a replica on this peer
	nodes := make(map[int]*Node)
	for _, shard := range metadata.Shards {
		for _, replica := range shard.Replicas {
			if replica.PeerID == cfg.NodeID {
				// This peer hosts a replica of this shard
				logger.Printf("Initializing replica for shard %d at %s", shard.ID, replica.RaftAddr)

				// Create a separate data directory for this shard
				shardDataDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", shard.ID))
				if err := os.MkdirAll(shardDataDir, 0755); err != nil {
					return nil, fmt.Errorf("failed to create data directory for shard %d: %w", shard.ID, err)
				}

				// Create a store for this shard
				kv, err := store.NewBoltStore(shardDataDir)
				if err != nil {
					return nil, fmt.Errorf("failed to create store for shard %d: %v", shard.ID, err)
				}

				// Create a config for this shard's Raft node
				shardCfg := Config{
					NodeID:    fmt.Sprintf("%s-shard-%d", cfg.NodeID, shard.ID),
					RaftAddr:  replica.RaftAddr,
					GRPCAddr:  cfg.GRPCAddr,
					DataDir:   shardDataDir,
					Bootstrap: cfg.Bootstrap,
					Peers:     make([]Peer, len(shard.Replicas)),
				}

				// Convert shard replicas to Peers
				for i, r := range shard.Replicas {
					shardCfg.Peers[i] = Peer{
						ID:       fmt.Sprintf("%s-shard-%d", r.PeerID, shard.ID),
						RaftAddr: r.RaftAddr,
					}
				}

				node, err := NewNode(shardCfg, kv, logger)
				if err != nil {
					return nil, fmt.Errorf("failed to create raft node for shard %d: %v", shard.ID, err)
				}

				nodes[shard.ID] = node
				break
			}
		}
	}

	logger.Printf("Initialized %d shard replicas", len(nodes))

	server := grpc.NewServer()
	svc := &grpcService{
		nodes:    nodes,
		metadata: metadata,
		cfg:      cfg,
		logger:   logger,
	}
	pb.RegisterKVServiceServer(server, svc)

	return &Server{
		nodes:    nodes,
		metadata: metadata,
		server:   server,
		logger:   logger,
	}, nil
}

func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

func (s *Server) Shutdown() error {
	s.server.GracefulStop()
	for shardID, node := range s.nodes {
		if err := node.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown raft node for shard %d: %v", shardID, err)
		}
	}
	return nil
}
