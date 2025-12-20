package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig_ValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	validConfig := `{
		"grpc_addr": "127.0.0.1:8001",
		"data_dir": "/tmp/kvdog/node1"
	}`

	if err := os.WriteFile(configPath, []byte(validConfig), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() returned unexpected error: %v", err)
	}

	if cfg.GRPCAddr != "127.0.0.1:8001" {
		t.Errorf("expected GRPCAddr to be '127.0.0.1:8001', got '%s'", cfg.GRPCAddr)
	}
	if cfg.DataDir != "/tmp/kvdog/node1" {
		t.Errorf("expected DataDir to be '/tmp/kvdog/node1', got '%s'", cfg.DataDir)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.json")
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read config file") {
		t.Errorf("expected error message to contain 'failed to read config file', got: %v", err)
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	invalidConfig := "invalid json here"

	if err := os.WriteFile(configPath, []byte(invalidConfig), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := LoadConfig(configPath)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse config file") {
		t.Errorf("expected error message to contain 'failed to parse config file', got: %v", err)
	}
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectedErr string
	}{
		{
			name: "missing grpc_addr",
			config: `{
				"node_id": "node1",
				"raft_addr": "127.0.0.1:7001",
				"data_dir": "/tmp/kvdog/node1"
			}`,
			expectedErr: "grpc_addr is required",
		},
		{
			name: "missing data_dir",
			config: `{
				"node_id": "node1",
				"raft_addr": "127.0.0.1:7001",
				"grpc_addr": "127.0.0.1:8001"
			}`,
			expectedErr: "data_dir is required",
		},
		{
			name: "empty grpc_addr",
			config: `{
				"node_id": "node1",
				"raft_addr": "127.0.0.1:7001",
				"grpc_addr": "",
				"data_dir": "/tmp/kvdog/node1"
			}`,
			expectedErr: "grpc_addr is required",
		},
		{
			name: "empty data_dir",
			config: `{
				"node_id": "node1",
				"raft_addr": "127.0.0.1:7001",
				"grpc_addr": "127.0.0.1:8001",
				"data_dir": ""
			}`,
			expectedErr: "data_dir is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.json")

			if err := os.WriteFile(configPath, []byte(tt.config), 0644); err != nil {
				t.Fatalf("failed to write config file: %v", err)
			}

			_, err := LoadConfig(configPath)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.expectedErr {
				t.Errorf("expected error '%s', got '%s'", tt.expectedErr, err.Error())
			}
		})
	}
}
