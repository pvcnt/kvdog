package transport

import (
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// shardStreamLayer wraps MuxStreamLayer to provide a shard-specific StreamLayer
type shardStreamLayer struct {
	shardID  int
	muxLayer *MuxStreamLayer
	acceptCh chan net.Conn
}

// newShardStreamLayer creates a new shard-specific stream layer wrapper
func newShardStreamLayer(shardID int, muxLayer *MuxStreamLayer) *shardStreamLayer {
	acceptCh := muxLayer.RegisterShard(shardID)

	return &shardStreamLayer{
		shardID:  shardID,
		muxLayer: muxLayer,
		acceptCh: acceptCh,
	}
}

// Dial establishes a connection to the given address for this shard
// It appends the shard ID to create a virtual address
func (s *shardStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	// Parse the address to extract peer address
	// The address might already be in virtual format (host:port/shardID) or just peer format (host:port)
	// We need to construct the virtual address for this shard
	peerAddr := string(address)

	// If address contains a shard ID, extract just the peer address
	if extracted, _, err := ParseVirtualAddress(peerAddr); err == nil {
		peerAddr = extracted
	}

	// Build virtual address with this shard's ID
	virtualAddr := BuildVirtualAddress(peerAddr, s.shardID)

	return s.muxLayer.Dial(virtualAddr, timeout)
}

// Accept waits for and returns the next connection to this shard
func (s *shardStreamLayer) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-s.acceptCh:
		if !ok {
			return nil, &net.OpError{
				Op:  "accept",
				Net: "tcp",
				Err: net.ErrClosed,
			}
		}
		return conn, nil
	}
}

// Close closes the stream layer (delegates to MuxStreamLayer)
func (s *shardStreamLayer) Close() error {
	// Don't close the MuxStreamLayer here - it's shared
	// The Manager will handle closing it
	return nil
}

// Addr returns the advertise address
func (s *shardStreamLayer) Addr() net.Addr {
	return s.muxLayer.Addr()
}
