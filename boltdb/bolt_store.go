/*
The KeyValue interface wrapper for BoltDB
*/

package boltdb

import (
	"bytes"
	"fmt"
	"log"

	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/kvi"
	"github.com/boltdb/bolt"
)

var loaded = kvgraph.AddKVDriver("bolt", BoltBuilder)
var graphBucket = []byte("graph")

// BoltBuilder creates a new bolt interface at `path`
func BoltBuilder(path string) (kvi.KVInterface, error) {
	log.Printf("Starting BoltDB")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(graphBucket) == nil {
			tx.CreateBucket(graphBucket)
		}
		return nil
	})
	return &BoltKV{
		db: db,
	}, nil
}

// BoltKV is an implementation of the KVStore for bolt
type BoltKV struct {
	db *bolt.DB
}

// Close closes the boltdb connection
func (boltkv *BoltKV) Close() error {
	return boltkv.db.Close()
}

// Delete removes a key/value from a kvstore
func (boltkv *BoltKV) Delete(id []byte) error {
	err := boltkv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		return b.Delete(id)
	})
	return err
}

// DeletePrefix deletes all elements in kvstore that begin with prefix `id`
func (boltkv *BoltKV) DeletePrefix(id []byte) error {
	err := boltkv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		odel := make([][]byte, 0, 100)
		c := b.Cursor()
		for k, _ := c.Seek([]byte(id)); bytes.HasPrefix(k, []byte(id)); k, _ = c.Next() {
			odel = append(odel, k)
		}
		for _, okey := range odel {
			b.Delete(okey)
		}
		return nil
	})
	return err
}

// HasKey returns true if the key is exists in kvstore
func (boltkv *BoltKV) HasKey(id []byte) bool {
	out := false
	boltkv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		d := b.Get([]byte(id))
		if d != nil {
			out = true
		}
		return nil
	})
	return out
}

// Set value in kvstore
func (boltkv *BoltKV) Set(id []byte, val []byte) error {
	err := boltkv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		b.Put(id, val)
		return nil
	})
	return err
}

// Update runs an alteration transaction of the kvstore
func (boltkv *BoltKV) Update(u func(tx kvi.KVTransaction) error) error {
	err := boltkv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		ktx := boltTransaction{tx, b}
		return u(ktx)
	})
	return err
}

// View returns an iterator for the kvstore
func (boltkv *BoltKV) View(u func(it kvi.KVIterator) error) error {
	err := boltkv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(graphBucket)
		ktx := &boltIterator{tx, b, b.Cursor(), nil, nil}
		return u(ktx)
	})
	return err
}

type boltTransaction struct {
	tx *bolt.Tx
	b  *bolt.Bucket
}

// Delete removes key `id` from the kv store
func (boltTrans boltTransaction) Delete(id []byte) error {
	return boltTrans.b.Delete(id)
}

func (boltTrans boltTransaction) Set(key, val []byte) error {
	b := boltTrans.tx.Bucket(graphBucket)
	return b.Put(key, val)
}

// Get retrieves the value of key `id`
func (boltTrans boltTransaction) Get(id []byte) ([]byte, error) {
	o := boltTrans.b.Get(id)
	if o == nil {
		return nil, fmt.Errorf("Not Found")
	}
	return copyBytes(o), nil
}

func (boltTrans boltTransaction) HasKey(id []byte) bool {
	b := boltTrans.tx.Bucket(graphBucket)
	d := b.Get([]byte(id))
	if d != nil {
		return true
	}
	return false
}

type boltIterator struct {
	tx    *bolt.Tx
	b     *bolt.Bucket
	c     *bolt.Cursor
	key   []byte
	value []byte
}

// Get retrieves the value of key `id`
func (boltIt *boltIterator) Get(id []byte) ([]byte, error) {
	o := boltIt.b.Get(id)
	if o == nil {
		return nil, fmt.Errorf("Not Found")
	}
	return copyBytes(o), nil
}

// Key returns the key the iterator is currently pointed at
func (boltIt *boltIterator) Key() []byte {
	return boltIt.key
}

// Value returns the valud of the iterator is currently pointed at
func (boltIt *boltIterator) Value() ([]byte, error) {
	return boltIt.value, nil
}

// Next move the iterator to the next key
func (boltIt *boltIterator) Next() error {
	k, v := boltIt.c.Next()
	if k == nil || v == nil {
		boltIt.key = nil
		boltIt.value = nil
		return nil
	}
	boltIt.key = copyBytes(k)
	boltIt.value = copyBytes(v)
	return nil
}

// Seek moves the iterator to a new location
func (boltIt *boltIterator) Seek(id []byte) error {
	k, v := boltIt.c.Seek(id)
	if k == nil || v == nil {
		boltIt.key = nil
		boltIt.value = nil
		return fmt.Errorf("Seek error")
	}
	boltIt.key = copyBytes(k)
	boltIt.value = copyBytes(v)
	return nil
}

// Valid returns true if iterator is still in valid location
func (boltIt *boltIterator) Valid() bool {
	if boltIt.key == nil || boltIt.value == nil {
		return false
	}
	return true
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
