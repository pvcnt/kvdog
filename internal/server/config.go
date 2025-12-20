package server

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the cluster configuration
type Config struct {
	// GRPCAddr is the address for client gRPC requests
	GRPCAddr string `json:"grpc_addr"`
	// DataDir is the directory where data is persisted
	DataDir string `json:"data_dir"`
}

// Load reads and parses a configuration file
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
	if cfg.GRPCAddr == "" {
		return cfg, fmt.Errorf("grpc_addr is required")
	}
	if cfg.DataDir == "" {
		return cfg, fmt.Errorf("data_dir is required")
	}

	return cfg, nil
}
