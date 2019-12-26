/*
The KeyValue interface wrapper for LevelDB
*/

package leveldb

import (
	"bytes"
	"fmt"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var loaded = kvi.AddKVDriver("level", NewKVInterface)

// NewKVInterface creates new LevelDB backed KVInterface at `path`
func NewKVInterface(path string, opts kvi.Options) (kvi.KVInterface, error) {
	log.Info("Starting LevelDB")

	var db *leveldb.DB
	var err error
	if opts.BulkLoad {
		o := opt.Options{}
		o.CompactionL0Trigger = 1 << 31
		db, err = leveldb.OpenFile(path, &o)
	} else {
		db, err = leveldb.OpenFile(path, nil)
	}
	if err != nil {
		return &LevelKV{}, err
	}
	return &LevelKV{db: db, opts: opts}, nil
}

// LevelKV implements the generic key value interface using the leveldb library
type LevelKV struct {
	db   *leveldb.DB
	opts kvi.Options
}

// Close database
func (l *LevelKV) Close() error {
	if l.opts.BulkLoad {
		l.db.CompactRange(util.Range{Start: nil, Limit: nil})
	}
	return l.db.Close()
}

// Get retrieves the value of key `id`
func (l *LevelKV) Get(id []byte) ([]byte, error) {
	return l.db.Get(id, nil)
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

// HasKey returns true if the key is exists in kvstore
func (l *LevelKV) HasKey(id []byte) bool {
	out, _ := l.db.Has(id, nil)
	return out
}

// Set value in kvstore
func (l *LevelKV) Set(id []byte, val []byte) error {
	return l.db.Put(id, val, nil)
}

// Update runs an alteration transaction of the kvstore
func (l *LevelKV) Update(u func(tx kvi.KVTransaction) error) error {
	tx, _ := l.db.OpenTransaction()
	ktx := levelTransaction{tx, l.db}
	defer tx.Commit()
	return u(ktx)
}

// BulkWrite is a copy of Update, with no special function yet...
func (l *LevelKV) BulkWrite(u func(tx kvi.KVBulkWrite) error) error {
	tx, _ := l.db.OpenTransaction()
	ktx := levelTransaction{tx, l.db}
	defer tx.Commit()
	return u(ktx)
}

type levelTransaction struct {
	tx *leveldb.Transaction
	db *leveldb.DB
}

func (ltx levelTransaction) Set(key, val []byte) error {
	return ltx.tx.Put(key, val, nil) //&opt.WriteOptions{NoWriteMerge: true})
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

// View run iterator on bolt keyvalue store
func (ltx levelTransaction) View(u func(it kvi.KVIterator) error) error {
	it := ltx.db.NewIterator(nil, nil)
	defer it.Release()
	lit := levelIterator{ltx.db, it, true, nil, nil}
	return u(&lit)
}

type levelIterator struct {
	db      *leveldb.DB
	it      iterator.Iterator
	forward bool
	key     []byte
	value   []byte
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

func (lit *levelIterator) Seek(id []byte) error {
	lit.forward = true
	if lit.it.Seek(id) {
		lit.key = copyBytes(lit.it.Key())
		lit.value = copyBytes(lit.it.Value())
		return nil
	}
	return fmt.Errorf("Invalid")
}

func (lit *levelIterator) SeekReverse(id []byte) error {
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
	lit := levelIterator{l.db, it, true, nil, nil}
	return u(&lit)
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
