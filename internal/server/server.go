package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"

	"github.com/pvcnt/kvdog/internal/store"
	"github.com/pvcnt/kvdog/internal/transport"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

// Server implements the KVServiceServer interface
type Server struct {
	nodes        map[int]*Node // Map of shard ID to Node
	metadata     Metadata
	server       *grpc.Server
	logger       *log.Logger
	transportMgr *transport.Manager
}

// NewServer creates a new KV server instance
func NewServer(cfg Config, metadata Metadata) (*Server, error) {
	log.Printf("Starting kvdog server with node_id=%s, raft_addr=%s, grpc_addr=%s",
		cfg.NodeID, cfg.RaftAddr, cfg.GRPCAddr)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", cfg.NodeID), log.LstdFlags)
	hcLogger := hclog.New(&hclog.LoggerOptions{
		Name:   cfg.NodeID,
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	// Resolve Raft address for transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}

	// Create shared transport manager for all shards
	transportMgr, err := transport.NewManager(cfg.RaftAddr, addr, hcLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport manager: %w", err)
	}

	// Create a Raft node for each shard that has a replica on this peer
	nodes := make(map[int]*Node)
	for _, shard := range metadata.Shards {
		for _, replica := range shard.Replicas {
			if replica.PeerID == cfg.NodeID {
				// This peer hosts a replica of this shard
				logger.Printf("Initializing replica for shard %d", shard.ID)

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

				// Create transport for this shard
				raftTransport, err := transportMgr.CreateTransport(shard.ID, 3, 10*time.Second)
				if err != nil {
					return nil, fmt.Errorf("failed to create transport for shard %d: %w", shard.ID, err)
				}

				// Create a config for this shard's Raft node
				shardCfg := Config{
					NodeID:    fmt.Sprintf("%s-shard-%d", cfg.NodeID, shard.ID),
					RaftAddr:  cfg.RaftAddr, // Not used in NewNode anymore, but keep for consistency
					GRPCAddr:  cfg.GRPCAddr,
					DataDir:   shardDataDir,
					Bootstrap: cfg.Bootstrap,
					Peers:     make([]Peer, len(shard.Replicas)),
				}

				// Convert shard replicas to Peers with virtual addresses
				for i, r := range shard.Replicas {
					// Find peer config to get raft_addr
					peerCfg := findPeerConfig(cfg.Peers, r.PeerID)
					if peerCfg == nil {
						return nil, fmt.Errorf("peer %s not found in config", r.PeerID)
					}
					shardCfg.Peers[i] = Peer{
						ID:       fmt.Sprintf("%s-shard-%d", r.PeerID, shard.ID),
						RaftAddr: fmt.Sprintf("%s/%d", peerCfg.RaftAddr, shard.ID), // Virtual address
						GRPCAddr: peerCfg.GRPCAddr,
					}
				}

				node, err := NewNode(shardCfg, kv, raftTransport, logger)
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
		nodes:        nodes,
		metadata:     metadata,
		server:       server,
		logger:       logger,
		transportMgr: transportMgr,
	}, nil
}

func (s *Server) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

func (s *Server) Shutdown() error {
	s.server.GracefulStop()

	// Shutdown transport manager first (closes all connections)
	if s.transportMgr != nil {
		if err := s.transportMgr.Shutdown(); err != nil {
			s.logger.Printf("Error shutting down transport manager: %v", err)
		}
	}

	// Then shutdown Raft nodes
	for shardID, node := range s.nodes {
		if err := node.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown raft node for shard %d: %v", shardID, err)
		}
	}
	return nil
}

// findPeerConfig finds a peer configuration by peer ID
func findPeerConfig(peers []Peer, peerID string) *Peer {
	for i := range peers {
		if peers[i].ID == peerID {
			return &peers[i]
		}
	}
	return nil
}
