package server

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the cluster configuration
type Config struct {
	// NodeID is the unique identifier for this node
	NodeID string `json:"node_id"`
	// RaftAddr is the address for Raft communication
	RaftAddr string `json:"raft_addr"`
	// GRPCAddr is the address for client gRPC requests
	GRPCAddr string `json:"grpc_addr"`
	// DataDir is the directory where data is persisted
	DataDir string `json:"data_dir"`
	// Bootstrap indicates whether this node should bootstrap the cluster
	Bootstrap bool `json:"bootstrap"`
	// Peers is the list of all cluster members (including self)
	Peers []Peer `json:"peers"`
}

// Peer represents a cluster member
type Peer struct {
	// ID is the unique identifier for this peer
	ID string `json:"id"`
	// RaftAddr is the address  of this peer for Raft communication
	RaftAddr string `json:"raft_addr"`
	// GRPCAddr is the address  of this peer for client gRPC requests
	GRPCAddr string `json:"grpc_addr"`
}

// LoadConfig reads and parses a configuration file
func LoadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate required fields
	if cfg.NodeID == "" {
		return cfg, fmt.Errorf("node_id is required")
	}
	if cfg.RaftAddr == "" {
		return cfg, fmt.Errorf("raft_addr is required")
	}
	if cfg.GRPCAddr == "" {
		return cfg, fmt.Errorf("grpc_addr is required")
	}
	if cfg.DataDir == "" {
		return cfg, fmt.Errorf("data_dir is required")
	}

	return cfg, nil
}
