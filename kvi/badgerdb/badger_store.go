/*
The KeyValue interface wrapper for BadgerDB
*/

package badgerdb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	log "github.com/sirupsen/logrus"
)

var loaded = kvgraph.AddKVDriver("badger", NewKVInterface)

// NewKVInterface creates new BoltDB backed KVInterface at `path`
func NewKVInterface(path string, kopts kvi.Options) (kvi.KVInterface, error) {
	log.Info("Starting BadgerDB")
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	opts := badger.Options{}
	opts = badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = path
	opts.ValueDir = path
	if kopts.BulkLoad {
		opts.SyncWrites = false
		opts.DoNotCompact = true // NOTE: this is a test value, it may need to be removed
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerKV{db: db}, nil
}

// BadgerKV is an implementation of the KVStore for badger
type BadgerKV struct {
	db *badger.DB
}

// Close closes the badger connection
func (badgerkv *BadgerKV) Close() error {
	log.Info("Closing BadgerDB")
	return badgerkv.db.Close()
}

// Get retrieves the value of key `id`
func (badgerkv *BadgerKV) Get(id []byte) ([]byte, error) {
	var out []byte
	err := badgerkv.db.View(func(tx *badger.Txn) error {
		dataValue, err := tx.Get(id)
		if err != nil {
			return err
		}
		d, _ := dataValue.Value()
		out = copyBytes(d)
		return nil
	})
	return out, err
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
		err := badgerkv.db.Update(func(tx *badger.Txn) error {
			it := tx.NewIterator(opts)
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
				wb = append(wb, copyBytes(it.Item().Key()))
			}
			it.Close()
			for _, i := range wb {
				err := tx.Delete(i)
				if err != nil {
					return err
				}
				found = true
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// HasKey returns true if the key is exists in kvstore
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

// Set value in kvstore
func (badgerkv *BadgerKV) Set(id []byte, val []byte) error {
	err := badgerkv.db.Update(func(tx *badger.Txn) error {
		return tx.Set(id, val)
	})
	return err
}

// Update runs an alteration transaction of the kvstore
func (badgerkv *BadgerKV) Update(u func(tx kvi.KVTransaction) error) error {
	err := badgerkv.db.Update(func(tx *badger.Txn) error {
		ktx := badgerTransaction{tx}
		return u(ktx)
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
	return badgerTrans.tx.Delete(id)
}

func (badgerTrans badgerTransaction) HasKey(id []byte) bool {
	_, err := badgerTrans.tx.Get(id)
	if err == nil {
		return true
	}
	return false
}

func (badgerTrans badgerTransaction) Get(id []byte) ([]byte, error) {
	dataValue, err := badgerTrans.tx.Get(id)
	if err != nil {
		return nil, err
	}
	d, _ := dataValue.Value()
	return copyBytes(d), nil
}

type badgerIterator struct {
	tx    *badger.Txn
	c     *badger.Iterator
	key   []byte
	value []byte
}

// Get retrieves the value of key `id`
func (badgerIt *badgerIterator) Get(id []byte) ([]byte, error) {
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
	if badgerIt.c != nil {
		badgerIt.c.Close()
	}
	opts := badger.DefaultIteratorOptions
	badgerIt.c = badgerIt.tx.NewIterator(opts)
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

// Seek moves the iterator to a new location
func (badgerIt *badgerIterator) SeekReverse(id []byte) error {
	if badgerIt.c != nil {
		badgerIt.c.Close()
	}
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	badgerIt.c = badgerIt.tx.NewIterator(opts)
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
func (badgerkv *BadgerKV) View(u func(it kvi.KVIterator) error) error {
	err := badgerkv.db.View(func(tx *badger.Txn) error {
		ktx := &badgerIterator{tx, nil, nil, nil}
		o := u(ktx)
		if ktx.c != nil {
			ktx.c.Close()
		}
		return o
	})
	return err
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
