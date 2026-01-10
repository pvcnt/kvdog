package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
)

const (
	// MagicNumber is the 4-byte magic number for the protocol header ("RAFT")
	MagicNumber uint32 = 0x52414654

	// HeaderSize is the size of the protocol header in bytes
	HeaderSize = 8
)

// WriteHeader writes the protocol header (magic number + shard ID) to the writer
func WriteHeader(w io.Writer, shardID int) error {
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:4], MagicNumber)
	binary.BigEndian.PutUint32(header[4:8], uint32(shardID))

	n, err := w.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if n != HeaderSize {
		return fmt.Errorf("incomplete header write: wrote %d bytes, expected %d", n, HeaderSize)
	}
	return nil
}

// ReadHeader reads and validates the protocol header, returning the shard ID
func ReadHeader(r io.Reader) (int, error) {
	header := make([]byte, HeaderSize)

	n, err := io.ReadFull(r, header)
	if err != nil {
		return 0, fmt.Errorf("failed to read header: %w", err)
	}
	if n != HeaderSize {
		return 0, fmt.Errorf("incomplete header read: read %d bytes, expected %d", n, HeaderSize)
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != MagicNumber {
		return 0, fmt.Errorf("invalid magic number: got 0x%08X, expected 0x%08X", magic, MagicNumber)
	}

	shardID := int(binary.BigEndian.Uint32(header[4:8]))
	return shardID, nil
}

// ParseVirtualAddress parses a virtual address in format "host:port/shardID"
// and returns the peer address and shard ID
func ParseVirtualAddress(addr string) (peerAddr string, shardID int, err error) {
	parts := strings.Split(addr, "/")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid virtual address format: %s (expected host:port/shardID)", addr)
	}

	peerAddr = parts[0]
	shardID, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid shard ID in address %s: %w", addr, err)
	}

	return peerAddr, shardID, nil
}

// BuildVirtualAddress constructs a virtual address from peer address and shard ID
func BuildVirtualAddress(peerAddr string, shardID int) raft.ServerAddress {
	return raft.ServerAddress(fmt.Sprintf("%s/%d", peerAddr, shardID))
}
