package store

import "io"

// Store defines the interface for key-value storage operations
type Store interface {
	io.Closer

	// Get retrieves a value for the given key
	// Returns the value and true if found, nil and false otherwise
	Get(key string) ([]byte, bool)

	// Set stores a value for the given key
	Set(key string, value []byte) error

	// Delete removes a key from the store
	Delete(key string) error
}
