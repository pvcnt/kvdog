package store

import (
	"bytes"
	"testing"
)

// StoreFactory is a function that creates a new Store instance for testing
type StoreFactory func(t *testing.T) Store

// RunStoreTests runs a comprehensive test suite against any Store implementation
func RunStoreTests(t *testing.T, name string, factory StoreFactory) {
	t.Run(name, func(t *testing.T) {
		t.Run("GetNonExistent", func(t *testing.T) {
			testGetNonExistent(t, factory)
		})
		t.Run("SetAndGet", func(t *testing.T) {
			testSetAndGet(t, factory)
		})
		t.Run("SetOverwrite", func(t *testing.T) {
			testSetOverwrite(t, factory)
		})
		t.Run("SetEmptyValue", func(t *testing.T) {
			testSetEmptyValue(t, factory)
		})
		t.Run("DeleteExisting", func(t *testing.T) {
			testDeleteExisting(t, factory)
		})
		t.Run("DeleteNonExistent", func(t *testing.T) {
			testDeleteNonExistent(t, factory)
		})
		t.Run("MultipleKeys", func(t *testing.T) {
			testMultipleKeys(t, factory)
		})
		t.Run("LargeValue", func(t *testing.T) {
			testLargeValue(t, factory)
		})
	})
}

func testGetNonExistent(t *testing.T, factory StoreFactory) {
	store := factory(t)

	value, found := store.Get("nonexistent")
	if found {
		t.Error("expected key to not be found")
	}
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
}

func testSetAndGet(t *testing.T, factory StoreFactory) {
	store := factory(t)

	key := "testkey"
	expectedValue := []byte("testvalue")

	if err := store.Set(key, expectedValue); err != nil {
		t.Errorf("expected no error writing key %s, got %v", key, err)
	}

	value, found := store.Get(key)
	if !found {
		t.Error("expected key to be found")
	}
	if !bytes.Equal(value, expectedValue) {
		t.Errorf("expected value %v, got %v", expectedValue, value)
	}
}

func testSetOverwrite(t *testing.T, factory StoreFactory) {
	store := factory(t)

	key := "testkey"
	value1 := []byte("value1")
	value2 := []byte("value2")

	if err := store.Set(key, value1); err != nil {
		t.Errorf("expected no error writing key %s, got %v", key, err)
	}
	if err := store.Set(key, value2); err != nil {
		t.Errorf("expected no error overwriting key %s, got %v", key, err)
	}

	value, found := store.Get(key)
	if !found {
		t.Error("expected key to be found")
	}
	if !bytes.Equal(value, value2) {
		t.Errorf("expected value %v, got %v", value2, value)
	}
}

func testSetEmptyValue(t *testing.T, factory StoreFactory) {
	store := factory(t)

	key := "testkey"
	emptyValue := make([]byte, 0)

	if err := store.Set(key, emptyValue); err != nil {
		t.Errorf("expected no error writing key %s, got %v", key, err)
	}

	value, found := store.Get(key)
	if !found {
		t.Error("expected key to be found")
	}
	if !bytes.Equal(value, emptyValue) {
		t.Errorf("expected empty value, got %v", value)
	}
}

func testDeleteExisting(t *testing.T, factory StoreFactory) {
	store := factory(t)

	key := "testkey"
	value := []byte("testvalue")

	if err := store.Set(key, value); err != nil {
		t.Errorf("expected no error writing key %s, got %v", key, err)
	}

	if err := store.Delete(key); err != nil {
		t.Errorf("expected no error deleting key, got %v", err)
	}

	if _, found := store.Get(key); found {
		t.Error("expected key to not be found after deletion")
	}
}

func testDeleteNonExistent(t *testing.T, factory StoreFactory) {
	store := factory(t)

	if err := store.Delete("nonexistent"); err != nil {
		t.Errorf("expected no error deleting non-existent key, got %v", err)
	}
}

func testMultipleKeys(t *testing.T, factory StoreFactory) {
	store := factory(t)

	keys := []string{"key1", "key2", "key3"}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	// Set all keys
	for i, key := range keys {
		if err := store.Set(key, values[i]); err != nil {
			t.Errorf("expected no error writing key %s, got %v", key, err)
		}
	}

	// Verify all keys
	for i, key := range keys {
		value, found := store.Get(key)
		if !found {
			t.Errorf("expected key %s to be found", key)
		}
		if !bytes.Equal(value, values[i]) {
			t.Errorf("expected value %v for key %s, got %v", values[i], key, value)
		}
	}

	// Delete one key
	if err := store.Delete(keys[1]); err != nil {
		t.Errorf("expected no error deleting key %s, got %v", keys[1], err)
	}

	// Verify remaining keys
	value, found := store.Get(keys[0])
	if !found || !bytes.Equal(value, values[0]) {
		t.Error("expected key1 to still exist")
	}

	if _, found = store.Get(keys[1]); found {
		t.Error("expected key2 to be deleted")
	}

	value, found = store.Get(keys[2])
	if !found || !bytes.Equal(value, values[2]) {
		t.Error("expected key3 to still exist")
	}
}

func testLargeValue(t *testing.T, factory StoreFactory) {
	store := factory(t)

	key := "largekey"
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	if err := store.Set(key, largeValue); err != nil {
		t.Errorf("expected no error writing key %s, got %v", key, err)
	}

	value, found := store.Get(key)
	if !found {
		t.Error("expected key to be found")
	}
	if !bytes.Equal(value, largeValue) {
		t.Error("large value not stored correctly")
	}
}
