package config

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Item is one stored config. MD5 is derived from Content at write time.
type Item struct {
	Content      string `json:"content"`
	MD5          string `json:"md5"`
	Type         string `json:"type,omitempty"`
	AppName      string `json:"appName,omitempty"`
	LastModified int64  `json:"lastModified"`
}

// Store is the bbolt-backed state the FSM applies into. It is also the durable
// record: Apply writes synchronously, so a publish that returned success
// survives a crash even before any snapshot.
type Store struct {
	db *bolt.DB
}

var configBucket = []byte("config")

func OpenStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("config: open store %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(configBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Put(key string, it Item) error {
	b, err := json.Marshal(it)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(configBucket).Put([]byte(key), b)
	})
}

func (s *Store) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(configBucket).Delete([]byte(key))
	})
}

// LoadAll reads the whole state (FSM startup).
func (s *Store) LoadAll() (map[string]Item, error) {
	out := map[string]Item{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(configBucket).ForEach(func(k, v []byte) error {
			var it Item
			if err := json.Unmarshal(v, &it); err != nil {
				return err
			}
			out[string(k)] = it
			return nil
		})
	})
	return out, err
}

// ReplaceAll swaps the whole state in one transaction (FSM Restore).
func (s *Store) ReplaceAll(items map[string]Item) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(configBucket); err != nil {
			return err
		}
		b, err := tx.CreateBucket(configBucket)
		if err != nil {
			return err
		}
		for k, it := range items {
			v, err := json.Marshal(it)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Close() error { return s.db.Close() }
