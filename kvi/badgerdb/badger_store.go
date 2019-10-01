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
	"github.com/bmeg/grip/log"
	"github.com/dgraph-io/badger"
)

var loaded = kvgraph.AddKVDriver("badger", NewKVInterface)

// NewKVInterface creates new BoltDB backed KVInterface at `path`
func NewKVInterface(path string, kopts kvi.Options) (kvi.KVInterface, error) {
	log.Info("Starting BadgerDB")
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		merr := os.MkdirAll(path, 0700)
		if merr != nil {
			return nil, err
		}
	}

	logger := log.GetLogger()
	sublogger := logger.WithFields(log.Fields{"namespace": "badger"})
	opts := badger.DefaultOptions(path)
	opts = opts.WithLogger(sublogger)
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
		err = dataValue.Value(func(d []byte) error {
			out = copyBytes(d)
			return nil
		})
		return err
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
	_ = badgerkv.db.View(func(tx *badger.Txn) error {
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

// BulkWrite returns a pointer to the badger bulk write method
func (badgerkv *BadgerKV) BulkWrite(u func(tx kvi.KVBulkWrite) error) error {
	bt := badgerkv.db.NewWriteBatch()
	defer bt.Cancel()
	ktx := badgerBulkWrite{bt}
	err := u(ktx)
	if err != nil {
		return err
	}
	err = bt.Flush()
	if err != nil {
		return err
	}
	return nil
}

type badgerTransaction struct {
	tx *badger.Txn
}

type badgerBulkWrite struct {
	bt *badger.WriteBatch
}

func (badgerBW badgerBulkWrite) Set(key, val []byte) error {
	return badgerBW.bt.Set(key, val)
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
	return err == nil
}

func (badgerTrans badgerTransaction) Get(id []byte) ([]byte, error) {
	dataValue, err := badgerTrans.tx.Get(id)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = dataValue.Value(func(d []byte) error {
		out = copyBytes(d)
		return nil
	})
	return out, err
}

func (badgerTrans badgerTransaction) View(u func(it kvi.KVIterator) error) error {
	ktx := newIterator(badgerTrans.tx)
	o := u(ktx)
	ktx.close()
	return o
}

type badgerIterator struct {
	tx      *badger.Txn
	c       *badger.Iterator
	key     []byte
	forward bool
}

func newIterator(tx *badger.Txn) *badgerIterator {
	return &badgerIterator{tx, nil, nil, true}
}

func (badgerIt *badgerIterator) close() {
	if badgerIt.c != nil {
		badgerIt.c.Close()
	}
	badgerIt.c = nil
}

func (badgerIt *badgerIterator) init(forward bool) {
	if badgerIt.c != nil && badgerIt.forward == forward {
		return
	}
	if badgerIt.c != nil {
		badgerIt.c.Close()
	}
	opts := badger.DefaultIteratorOptions
	opts.Reverse = !forward
	opts.PrefetchValues = false
	opts.PrefetchSize = 10
	badgerIt.c = badgerIt.tx.NewIterator(opts)
	badgerIt.forward = forward
}

// Get retrieves the value of key `id`
func (badgerIt *badgerIterator) Get(id []byte) ([]byte, error) {
	dataValue, err := badgerIt.tx.Get(id)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = dataValue.Value(func(d []byte) error {
		out = copyBytes(d)
		return nil
	})
	return out, err
}

// Key returns the key the iterator is currently pointed at
func (badgerIt *badgerIterator) Key() []byte {
	return badgerIt.key
}

// Value returns the valud of the iterator is currently pointed at
func (badgerIt *badgerIterator) Value() ([]byte, error) {
	var out []byte
	err := badgerIt.c.Item().Value(func(d []byte) error {
		out = copyBytes(d)
		return nil
	})
	return out, err
}

// Next move the iterator to the next key
func (badgerIt *badgerIterator) Next() error {
	badgerIt.c.Next()
	if !badgerIt.c.Valid() {
		badgerIt.key = nil
		return fmt.Errorf("Invalid")
	}
	k := badgerIt.c.Item().Key()
	badgerIt.key = copyBytes(k)
	return nil
}

// Seek moves the iterator to a new location
func (badgerIt *badgerIterator) Seek(id []byte) error {
	badgerIt.init(true)
	badgerIt.c.Seek(id)
	if !badgerIt.c.Valid() {
		return fmt.Errorf("Invalid")
	}
	k := badgerIt.c.Item().Key()
	badgerIt.key = copyBytes(k)
	return nil
}

// Seek moves the iterator to a new location
func (badgerIt *badgerIterator) SeekReverse(id []byte) error {
	badgerIt.init(false)
	badgerIt.c.Seek(id)
	if !badgerIt.c.Valid() {
		return fmt.Errorf("Invalid")
	}
	k := badgerIt.c.Item().Key()
	badgerIt.key = copyBytes(k)
	return nil
}

// Valid returns true if iterator is still in valid location
func (badgerIt *badgerIterator) Valid() bool {
	return badgerIt.key != nil
}

// View run iterator on bolt keyvalue store
func (badgerkv *BadgerKV) View(u func(it kvi.KVIterator) error) error {
	err := badgerkv.db.View(func(tx *badger.Txn) error {
		ktx := newIterator(tx)
		o := u(ktx)
		ktx.close()
		return o
	})
	return err
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
