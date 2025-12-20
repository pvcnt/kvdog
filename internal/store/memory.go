package store

import (
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
