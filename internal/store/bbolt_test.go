package store

import (
	"testing"
)

func TestBoltStore(t *testing.T) {
	factory := func(t *testing.T) Store {
		store, err := NewBoltStore(t.TempDir())
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		t.Cleanup(func() {
			store.Close()
		})
		return store
	}
	RunStoreTests(t, "BoltStore", factory)
}
