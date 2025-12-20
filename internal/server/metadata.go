package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

const (
	// ReplicationFactor is the number of replicas each shard must have
	ReplicationFactor = 2
)

// Metadata contains the sharding configuration for the cluster
type Metadata struct {
	// Shards is the list of all shards in the cluster
	Shards []Shard `json:"shards"`
}

// Shard represents a single shard with its key range and replicas
type Shard struct {
	// ID is the unique identifier for this shard
	ID int `json:"id"`
	// KeyRangeStart is the starting hash value (inclusive) for this shard
	KeyRangeStart uint64 `json:"key_range_start"`
	// KeyRangeEnd is the ending hash value (exclusive) for this shard
	KeyRangeEnd uint64 `json:"key_range_end"`
	// Replicas is the list of replicas for this shard
	Replicas []Replica `json:"replicas"`
}

// Replica represents a single replica of a shard
type Replica struct {
	// PeerID is the identifier of the peer hosting this replica
	PeerID string `json:"peer_id"`
	// RaftAddr is the Raft address for this replica
	RaftAddr string `json:"raft_addr"`
}

// LoadMetadata reads and parses a metadata file
func LoadMetadata(path string) (Metadata, error) {
	var metadata Metadata
	data, err := os.ReadFile(path)
	if err != nil {
		return metadata, fmt.Errorf("failed to read metadata file: %w", err)
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, fmt.Errorf("failed to parse metadata file: %w", err)
	}

	// Validate metadata
	if err := validateMetadata(metadata); err != nil {
		return metadata, fmt.Errorf("invalid metadata: %w", err)
	}

	return metadata, nil
}

// validateMetadata validates the metadata configuration
func validateMetadata(m Metadata) error {
	if len(m.Shards) == 0 {
		return fmt.Errorf("at least one shard is required")
	}

	// Track seen shard IDs to detect duplicates
	seenIDs := make(map[int]bool)

	for i, shard := range m.Shards {
		// Check for duplicate IDs
		if seenIDs[shard.ID] {
			return fmt.Errorf("duplicate shard ID %d", shard.ID)
		}
		seenIDs[shard.ID] = true

		// Validate key range
		if shard.KeyRangeStart >= shard.KeyRangeEnd {
			return fmt.Errorf("shard %d: key_range_start (%d) must be less than key_range_end (%d)",
				shard.ID, shard.KeyRangeStart, shard.KeyRangeEnd)
		}

		// Validate replicas
		if len(shard.Replicas) != ReplicationFactor {
			return fmt.Errorf("shard %d: expected %d replicas, got %d", shard.ID, ReplicationFactor, len(shard.Replicas))
		}

		// Track seen peer IDs within this shard to detect duplicates
		seenPeers := make(map[string]bool)
		for j, replica := range shard.Replicas {
			if replica.PeerID == "" {
				return fmt.Errorf("shard %d, replica %d: peer_id is required", shard.ID, j)
			}
			if replica.RaftAddr == "" {
				return fmt.Errorf("shard %d, replica %d: raft_addr is required", shard.ID, j)
			}

			// Check for duplicate peer IDs within the same shard
			if seenPeers[replica.PeerID] {
				return fmt.Errorf("shard %d: duplicate peer_id %s in replicas", shard.ID, replica.PeerID)
			}
			seenPeers[replica.PeerID] = true
		}

		// Validate that key ranges don't overlap with previous shards
		for k := 0; k < i; k++ {
			other := m.Shards[k]
			// Check for overlap: [start1, end1) overlaps with [start2, end2) if:
			// start1 < end2 AND start2 < end1
			if shard.KeyRangeStart < other.KeyRangeEnd && other.KeyRangeStart < shard.KeyRangeEnd {
				return fmt.Errorf("shard %d key range [%d, %d) overlaps with shard %d key range [%d, %d)",
					shard.ID, shard.KeyRangeStart, shard.KeyRangeEnd,
					other.ID, other.KeyRangeStart, other.KeyRangeEnd)
			}
		}
	}

	return nil
}

// FindShard returns the shard that should handle the given key
func (m *Metadata) FindShard(key string) (*Shard, error) {
	// Hash the key using SHA256 and convert the first 8 bytes to a uint64.
	hash := sha256.Sum256([]byte(key))
	hashValue := binary.BigEndian.Uint64(hash[:8])

	for i := range m.Shards {
		shard := &m.Shards[i]
		if hashValue >= shard.KeyRangeStart && hashValue < shard.KeyRangeEnd {
			return shard, nil
		}
	}
	return nil, fmt.Errorf("no shard found for key %q (hash: %d)", key, hashValue)
}
