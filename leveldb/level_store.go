package labeldb

import (
	"bytes"
	"fmt"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/kvi"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"log"
)

// LevelBuilder creates new badger interface at `path`
// driver at `path`
func LevelBuilder(path string) (kvi.KVInterface, error) {
	log.Printf("Starting LevelDB")
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		log.Printf("Error: %s", err)
		return &LevelKV{}, err
	}
	o := &LevelKV{db: db}
	return o, err
}

var loaded = kvgraph.AddKVDriver("level", LevelBuilder)

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// LevelKV implements the generic key value interface using the leveldb library
type LevelKV struct {
	db *leveldb.DB
}

// Close database
func (l *LevelKV) Close() error {
	return l.db.Close()
}

// Delete removes a key/value from a kvstore
func (l *LevelKV) Delete(id []byte) error {
	return l.db.Delete(id, nil)
}

// DeletePrefix deletes all elements in kvstore that begin with prefix `id`
func (l *LevelKV) DeletePrefix(prefix []byte) error {
	deleteBlockSize := 10000
	for found := true; found; {
		found = false
		wb := make([][]byte, 0, deleteBlockSize)
		tx, _ := l.db.OpenTransaction()
		it := tx.NewIterator(nil, nil)
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
			wb = append(wb, copyBytes(it.Key()))
		}
		it.Release()
		for _, i := range wb {
			tx.Delete(i, nil)
			found = true
		}
		tx.Commit()
	}
	return nil
}

// HasKey returns true if the key is exists in kv store
func (l *LevelKV) HasKey(id []byte) bool {
	out, _ := l.db.Has(id, nil)
	return out
}

// Set value in kv store
func (l *LevelKV) Set(id []byte, val []byte) error {
	return l.db.Put(id, val, nil)
}

type levelTransaction struct {
	tx *leveldb.Transaction
}

func (ltx levelTransaction) Set(key, val []byte) error {
	return ltx.tx.Put(key, val, nil)
}

// Delete removes key `id` from the kv store
func (ltx levelTransaction) Delete(id []byte) error {
	return ltx.tx.Delete(id, nil)
}

func (ltx levelTransaction) HasKey(id []byte) bool {
	out, _ := ltx.tx.Has(id, nil)
	return out
}

func (ltx levelTransaction) Get(id []byte) ([]byte, error) {
	o, err := ltx.tx.Get(id, nil)
	if o == nil || err != nil {
		return nil, err
	}
	return copyBytes(o), nil
}

// Update runs an alteration transition of the level kv store
func (l *LevelKV) Update(u func(tx kvi.KVTransaction) error) error {
	tx, _ := l.db.OpenTransaction()
	ktx := levelTransaction{tx}
	defer tx.Commit()
	return u(ktx)
}

type levelIterator struct {
	db    *leveldb.DB
	it    iterator.Iterator
	key   []byte
	value []byte
}

// Get retrieves the value of key `id`
func (lit *levelIterator) Get(id []byte) ([]byte, error) {
	return lit.db.Get(id, nil)
}

// Key returns the key the iterator is currently pointed at
func (lit *levelIterator) Key() []byte {
	return lit.key
}

// Value returns the valud of the iterator is currently pointed at
func (lit *levelIterator) Value() ([]byte, error) {
	return lit.value, nil
}

// Next move the iterator to the next key
func (lit *levelIterator) Next() error {
	more := lit.it.Next()
	if !more {
		lit.key = nil
		lit.value = nil
		return fmt.Errorf("Invalid")
	}
	lit.key = lit.it.Key()
	lit.value = lit.it.Value()
	return nil
}

func (lit *levelIterator) Seek(id []byte) error {
	if lit.it.Seek(id) {
		lit.key = lit.it.Key()
		lit.value = lit.it.Value()
		return nil
	}
	return fmt.Errorf("Invalid")
}

// Valid returns true if iterator is still in valid location
func (lit *levelIterator) Valid() bool {
	if lit.key == nil || lit.value == nil {
		return false
	}
	return true
}

// View run iterator on bolt keyvalue store
func (l *LevelKV) View(u func(it kvi.KVIterator) error) error {
	it := l.db.NewIterator(nil, nil)
	defer it.Release()
	lit := levelIterator{l.db, it, nil, nil}
	return u(&lit)
}
