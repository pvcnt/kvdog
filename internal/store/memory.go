package store

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// memoryStore is an in-memory implementation of the Store interface
type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore creates a new in-memory store
func NewMemoryStore() Store {
	return &memoryStore{
		data: make(map[string][]byte),
	}
}

func (m *memoryStore) Close() error {
	return nil
}

func (m *memoryStore) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, found := m.data[key]
	return value, found
}

func (m *memoryStore) Set(key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

func (m *memoryStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *memoryStore) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &memorySnapshot{m.data}
}

func (m *memoryStore) Restore(r io.Reader) error {
	d := make(map[string][]byte)
	if err := json.NewDecoder(r).Decode(&d); err != nil {
		return fmt.Errorf("failed to unmarshal data: %v", err)
	}
	m.data = d
	return nil
}

type memorySnapshot struct {
	data map[string][]byte
}

func (m *memorySnapshot) Write(w io.Writer) (int64, error) {
	b, err := json.Marshal(m.data)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal data: %v", err)
	}
	var n int
	if n, err = w.Write(b); err != nil {
		return int64(n), fmt.Errorf("failed to write data: %v", err)
	}
	return int64(n), nil
}
