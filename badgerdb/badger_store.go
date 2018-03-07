package badgerdb

import (
	"bytes"
	"fmt"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/dgraph-io/badger"
	"log"
	"os"
)

// BadgerBuilder creates new badger interface at `path`
// driver at `path`
func BadgerBuilder(path string) (kvgraph.KVInterface, error) {
	log.Printf("Starting BadgerDB")
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	opts := badger.Options{}
	opts = badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	o := &BadgerKV{db: db}
	return o, nil
}

var loaded = kvgraph.AddKVDriver("badger", BadgerBuilder)

func bytesCopy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// BadgerKV is an implementation of the KVStore for badger
type BadgerKV struct {
	db *badger.DB
}

// Close closes the boltdb
func (badgerkv *BadgerKV) Close() error {
	return badgerkv.db.Close()
}

// Delete removes a key/value from a kvstore
func (badgerkv *BadgerKV) Delete(id []byte) error {
	err := badgerkv.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(id)
	})
	return err
}

// DeletePrefix deletes all elements in kvstore that begin with prefix `id`
func (badgerkv *BadgerKV) DeletePrefix(prefix []byte) error {
	deleteBlockSize := 10000

	for found := true; found; {
		found = false
		wb := make([][]byte, 0, deleteBlockSize)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		badgerkv.db.Update(func(tx *badger.Txn) error {
			it := tx.NewIterator(opts)
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
				wb = append(wb, bytesCopy(it.Item().Key()))
			}
			it.Close()
			for _, i := range wb {
				tx.Delete(i)
				found = true
			}
			return nil
		})
	}
	return nil
}

// HasKey returns true if the key is exists in kv store
func (badgerkv *BadgerKV) HasKey(id []byte) bool {
	out := false
	badgerkv.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get(id)
		if err == nil {
			out = true
		}
		return nil
	})
	return out
}

// Set value in kv store
func (badgerkv *BadgerKV) Set(id []byte, val []byte) error {
	err := badgerkv.db.Update(func(tx *badger.Txn) error {
		return tx.Set(id, val)
	})
	return err
}

type badgerTransaction struct {
	tx *badger.Txn
}

func (badgerTrans badgerTransaction) Set(key, val []byte) error {
	return badgerTrans.tx.Set(key, val)
}

// Delete removes key `id` from the kv store
func (badgerTrans badgerTransaction) Delete(id []byte) error {
	if err := badgerTrans.tx.Delete(id); err != nil {
		return err
	}
	return nil
}

func (badgerTrans badgerTransaction) HasKey(id []byte) bool {
	_, err := badgerTrans.tx.Get(id)
	if err == nil {
		return true
	}
	return false
}

// Update runs an alteration transition of the bolt kv store
func (badgerkv *BadgerKV) Update(u func(tx kvgraph.KVTransaction) error) error {
	err := badgerkv.db.Update(func(tx *badger.Txn) error {
		ktx := badgerTransaction{tx}
		return u(ktx)
	})
	return err
}

type badgerIterator struct {
	tx    *badger.Txn
	c     *badger.Iterator
	key   []byte
	value []byte
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// Get retrieves the value of key `id`
func (badgerIt *badgerIterator) Get(id []byte) ([]byte, error) {
	o, err := badgerIt.tx.Get(id)
	if o == nil || err != nil {
		return nil, fmt.Errorf("Not Found")
	}
	dataValue, err := badgerIt.tx.Get(id)
	if err != nil {
		return nil, err
	}
	d, _ := dataValue.Value()
	return copyBytes(d), nil
}

// Key returns the key the iterator is currently pointed at
func (badgerIt *badgerIterator) Key() []byte {
	return badgerIt.key
}

// Value returns the valud of the iterator is currently pointed at
func (badgerIt *badgerIterator) Value() ([]byte, error) {
	return badgerIt.value, nil
}

// Next move the iterator to the next key
func (badgerIt *badgerIterator) Next() error {
	badgerIt.c.Next()
	if !badgerIt.c.Valid() {
		badgerIt.key = nil
		badgerIt.value = nil
		return fmt.Errorf("Invalid")
	}
	k := badgerIt.c.Item().Key()
	badgerIt.key = copyBytes(k)
	v, _ := badgerIt.c.Item().Value()
	badgerIt.value = copyBytes(v)
	return nil
}

// Seek moves the iterator to a new location
func (badgerIt *badgerIterator) Seek(id []byte) error {
	badgerIt.c.Seek(id)
	if !badgerIt.c.Valid() {
		return fmt.Errorf("Invalid")
	}
	k := badgerIt.c.Item().Key()
	badgerIt.key = copyBytes(k)
	v, _ := badgerIt.c.Item().Value()
	badgerIt.value = copyBytes(v)
	return nil
}

// Valid returns true if iterator is still in valid location
func (badgerIt *badgerIterator) Valid() bool {
	if badgerIt.key == nil || badgerIt.value == nil {
		return false
	}
	return true
}

// View run iterator on bolt keyvalue store
func (badgerkv *BadgerKV) View(u func(it kvgraph.KVIterator) error) error {
	err := badgerkv.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := tx.NewIterator(opts)
		ktx := &badgerIterator{tx, it, nil, nil}
		o := u(ktx)
		it.Close()
		return o
	})
	return err
}
