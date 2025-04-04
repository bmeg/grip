package leveldb

import (
	"bytes"
	"fmt"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
)

var mem_loaded = kvi.AddKVDriver("memdb", NewMemKVInterface)

type LevelMemKV struct {
	db *memdb.DB
}

// NewKVInterface creates new LevelDB backed KVInterface at `path`
func NewMemKVInterface(path string, opts kvi.Options) (kvi.KVInterface, error) {
	log.Info("Starting In-Memory LevelDB")
	db := memdb.New(comparer.DefaultComparer, 1000)
	return &LevelMemKV{db: db}, nil
}

// BulkWrite implements kvi.KVInterface.
func (l *LevelMemKV) BulkWrite(u func(bl kvi.KVBulkWrite) error) error {
	ktx := &memIterator{l.db, nil, true, nil, nil}
	return u(ktx)
}

// Close implements kvi.KVInterface.
func (l *LevelMemKV) Close() error {
	return nil
}

// Delete implements kvi.KVInterface.
func (l *LevelMemKV) Delete(key []byte) error {
	return l.db.Delete(key)
}

// DeletePrefix implements kvi.KVInterface.
func (l *LevelMemKV) DeletePrefix(prefix []byte) error {
	deleteBlockSize := 10000
	for found := true; found; {
		found = false
		wb := make([][]byte, 0, deleteBlockSize)
		it := l.db.NewIterator(nil)
		for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
			wb = append(wb, copyBytes(it.Key()))
		}
		it.Release()
		for _, i := range wb {
			l.db.Delete(i)
			found = true
		}
	}
	return nil

}

// Get implements kvi.KVInterface.
func (l *LevelMemKV) Get(key []byte) ([]byte, error) {
	return l.db.Get(key)
}

// HasKey implements kvi.KVInterface.
func (l *LevelMemKV) HasKey(key []byte) bool {
	_, err := l.db.Get(key)
	return err == nil
}

// Set implements kvi.KVInterface.
func (l *LevelMemKV) Set(key []byte, value []byte) error {
	return l.db.Put(key, value)
}

// Update implements kvi.KVInterface.
func (l *LevelMemKV) Update(func(tx kvi.KVTransaction) error) error {
	panic("unimplemented")
}

// View implements kvi.KVInterface.
func (l *LevelMemKV) View(u func(it kvi.KVIterator) error) error {
	it := l.db.NewIterator(nil)
	defer it.Release()
	lit := memIterator{l.db, it, true, nil, nil}
	return u(&lit)
}

type memIterator struct {
	db      *memdb.DB
	it      iterator.Iterator
	forward bool
	key     []byte
	value   []byte
}

// Get retrieves the value of key `id`
func (lit *memIterator) Get(id []byte) ([]byte, error) {
	return lit.db.Get(id)
}

func (lit *memIterator) Set(key, val []byte) error {
	return lit.db.Put(key, val)
}

// Key returns the key the iterator is currently pointed at
func (lit *memIterator) Key() []byte {
	return lit.key
}

// Value returns the valud of the iterator is currently pointed at
func (lit *memIterator) Value() ([]byte, error) {
	return lit.value, nil
}

// Next move the iterator to the next key
func (lit *memIterator) Next() error {
	var more bool
	if lit.forward {
		more = lit.it.Next()
	} else {
		more = lit.it.Prev()
	}
	if !more {
		lit.key = nil
		lit.value = nil
		return fmt.Errorf("Invalid")
	}
	lit.key = copyBytes(lit.it.Key())
	lit.value = copyBytes(lit.it.Value())
	return nil
}

func (lit *memIterator) Seek(id []byte) error {
	lit.forward = true
	if lit.it.Seek(id) {
		lit.key = copyBytes(lit.it.Key())
		lit.value = copyBytes(lit.it.Value())
		return nil
	}
	return fmt.Errorf("Invalid")
}

func (lit *memIterator) SeekReverse(id []byte) error {
	lit.forward = false
	if lit.it.Seek(id) {
		//Level iterator will land on the first value above the request
		//if we're there, move once to get below start request
		if bytes.Compare(id, lit.it.Key()) < 0 {
			lit.it.Prev()
		}
		lit.key = copyBytes(lit.it.Key())
		lit.value = copyBytes(lit.it.Value())
		return nil
	}
	return fmt.Errorf("Invalid")
}

// Valid returns true if iterator is still in valid location
func (lit *memIterator) Valid() bool {
	if lit.key == nil || lit.value == nil {
		return false
	}
	return true
}
