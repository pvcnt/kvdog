package transport

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// Manager manages all Raft transports for a peer
type Manager struct {
	muxLayer *MuxStreamLayer
	logger   hclog.Logger

	// Per-shard transports
	transports     map[int]raft.Transport
	transportsLock sync.RWMutex

	// Shutdown coordination
	shutdownOnce sync.Once
}

// NewManager creates a new transport manager
func NewManager(bindAddr string, advertise net.Addr, logger hclog.Logger) (*Manager, error) {
	// Create shared multiplexing layer
	muxLayer, err := NewMuxStreamLayer(bindAddr, advertise, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create mux stream layer: %w", err)
	}

	return &Manager{
		muxLayer:   muxLayer,
		logger:     logger,
		transports: make(map[int]raft.Transport),
	}, nil
}

// CreateTransport creates a new Raft transport for the specified shard
func (tm *Manager) CreateTransport(shardID int, maxPool int, timeout time.Duration) (raft.Transport, error) {
	tm.transportsLock.Lock()
	defer tm.transportsLock.Unlock()

	// Check if transport already exists for this shard
	if transport, exists := tm.transports[shardID]; exists {
		return transport, nil
	}

	// Create shard-specific stream layer wrapper
	streamLayer := newShardStreamLayer(shardID, tm.muxLayer)

	// Create NetworkTransport for this shard
	transport := raft.NewNetworkTransportWithLogger(
		streamLayer,
		maxPool,
		timeout,
		tm.logger,
	)

	tm.transports[shardID] = transport
	tm.logger.Info("created transport for shard", "shard", shardID)

	return transport, nil
}

// Shutdown gracefully shuts down all transports and the mux layer
func (tm *Manager) Shutdown() error {
	var shutdownErr error

	tm.shutdownOnce.Do(func() {
		tm.logger.Info("shutting down transport manager")

		// Close mux layer - this will close all underlying connections and streams
		// Individual transports don't need to be closed separately
		if err := tm.muxLayer.Close(); err != nil {
			tm.logger.Error("error closing mux layer", "error", err)
			shutdownErr = err
		}

		tm.logger.Info("transport manager shutdown complete")
	})

	return shutdownErr
}
