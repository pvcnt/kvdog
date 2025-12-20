package store

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

var (
	// defaultBucket is the name of the bucket used to store key-value pairs
	defaultBucket = []byte("kvdog")
)

// boltStore is a persistent implementation of the Store interface using bbolt
type boltStore struct {
	dir string
	db  *bbolt.DB
}

// NewBoltStore creates a new bbolt-backed store
func NewBoltStore(dir string) (Store, error) {
	db, err := openBoltDb(filepath.Join(dir, "kv.db"))
	if err != nil {
		return nil, err
	}
	return &boltStore{
		dir: dir,
		db:  db,
	}, nil
}

func openBoltDb(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}
	return db, nil
}

func (b *boltStore) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *boltStore) Get(key string) ([]byte, bool) {
	var value []byte
	var found bool

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		if bucket == nil {
			return nil
		}

		val := bucket.Get([]byte(key))
		if val != nil {
			// Make a copy since the value is only valid during the transaction
			value = make([]byte, len(val))
			copy(value, val)
			found = true
		}
		return nil
	})

	if err != nil {
		return nil, false
	}

	return value, found
}

func (b *boltStore) Set(key string, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		return bucket.Put([]byte(key), value)
	})
}

func (b *boltStore) Delete(key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(defaultBucket)
		if bucket == nil {
			return nil
		}

		// Check if the key exists before deleting
		if bucket.Get([]byte(key)) != nil {
			return bucket.Delete([]byte(key))
		}
		return nil
	})
}

func (b *boltStore) Snapshot() Snapshot {
	return &boltSnapshot{b.db}
}

func (b *boltStore) Restore(r io.Reader) error {
	// Write the snapshot to a file before opening it as a new database.
	path := filepath.Join(b.dir, fmt.Sprintf("%v-%v.db", time.Now().Nanosecond(), rand.Int()))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", path, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to copy to file %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %v", path, err)
	}
	if b.db, err = openBoltDb(path); err != nil {
		return err
	}
	return nil
}

type boltSnapshot struct {
	db *bbolt.DB
}

func (b *boltSnapshot) Write(w io.Writer) (int64, error) {
	var n int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		var err error
		n, err = tx.WriteTo(w)
		return err
	})
	return n, err
}
