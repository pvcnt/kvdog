package store

import (
	"testing"
)

func TestMemoryStore(t *testing.T) {
	factory := func(t *testing.T) Store {
		return NewMemoryStore()
	}
	RunStoreTests(t, "MemoryStore", factory)
}
