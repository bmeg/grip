/*
The KeyValue interface wrapper for BadgerDB
*/

package pebbledb

import (
	"bytes"
	"io"

	//"fmt"
	//"os"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"

	"github.com/cockroachdb/pebble"
)

var loaded = kvi.AddKVDriver("pebble", NewKVInterface)

// PebbleKV is an implementation of the KVStore for badger
type PebbleKV struct {
	db *pebble.DB
}

// NewKVInterface creates new BoltDB backed KVInterface at `path`
func NewKVInterface(path string, kopts kvi.Options) (kvi.KVInterface, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleKV{db: db}, nil
}

// Close closes the badger connection
func (pdb *PebbleKV) Close() error {
	log.Info("Closing PebbleDB")
	return pdb.db.Close()
}

// Get retrieves the value of key `id`
func (pdb *PebbleKV) Get(id []byte) ([]byte, error) {
	v, c, err := pdb.db.Get(id)
	if err != nil {
		return nil, err
	}
	out := copyBytes(v)
	c.Close()
	return out, nil
}

// Delete removes a key/value from a kvstore
func (pdb *PebbleKV) Delete(id []byte) error {
	return pdb.db.Delete(id, nil)
}

// DeletePrefix deletes all elements in kvstore that begin with prefix `id`
func (pdb *PebbleKV) DeletePrefix(prefix []byte) error {
	deleteBlockSize := 10000
	for found := true; found; {
		found = false
		wb := make([][]byte, 0, deleteBlockSize)
		it := pdb.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
		for ; it.Valid() && bytes.HasPrefix(it.Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
			wb = append(wb, copyBytes(it.Key()))
		}
		it.Close()
		for _, i := range wb {
			err := pdb.db.Delete(i, nil)
			if err != nil {
				return err
			}
			found = true
		}
	}
	return nil
}

// HasKey returns true if the key is exists in kvstore
func (pdb *PebbleKV) HasKey(id []byte) bool {
	_, c, err := pdb.db.Get(id)
	c.Close()
	if err != nil {
		return false
	}
	return true
}

// Set value in kvstore
func (pdb *PebbleKV) Set(id []byte, val []byte) error {
	return pdb.db.Set(id, val, nil)
}

func (pdb *PebbleKV) View(u func(it kvi.KVIterator) error) error {
	it := pdb.db.NewIter(&pebble.IterOptions{})
	pit := &pebbleIterator{pdb.db, it, true, nil, nil}
	err := u(pit)
	it.Close()
	return err
}

type pebbleTransaction struct {
	db *pebble.DB
}

func (ptx pebbleTransaction) HasKey(id []byte) bool {
	_, c, err := ptx.db.Get(id)
	c.Close()
	if err != nil {
		return false
	}
	return true
}

func (ptx pebbleTransaction) Get(id []byte) ([]byte, error) {
	v, c, err := ptx.db.Get(id)
	if err != nil {
		return nil, err
	}
	out := copyBytes(v)
	c.Close()
	return out, nil
}

func (ptx pebbleTransaction) Set(id []byte, val []byte) error {
	return ptx.db.Set(id, val, nil)
}

// Delete removes key `id` from the kv store
func (ptx pebbleTransaction) Delete(id []byte) error {
	return ptx.db.Delete(id, nil)
}

func (ptx pebbleTransaction) View(u func(it kvi.KVIterator) error) error {
	it := ptx.db.NewIter(&pebble.IterOptions{})
	pit := &pebbleIterator{ptx.db, it, true, nil, nil}
	err := u(pit)
	it.Close()
	return err
}

type pebbleIterator struct {
	db      *pebble.DB
	iter    *pebble.Iterator
	forward bool
	key     []byte
	value   []byte
}

func (pit *pebbleIterator) Get(id []byte) ([]byte, error) {
	v, c, err := pit.db.Get(id)
	if err != nil {
		return nil, err
	}
	out := copyBytes(v)
	c.Close()
	return out, nil
}

func (pit *pebbleIterator) Key() []byte {
	return pit.key
}

func (pit *pebbleIterator) Value() ([]byte, error) {
	return pit.value, nil
}

// Next move the iterator to the next key
func (pit *pebbleIterator) Next() error {
	if pit.forward {
		if !pit.iter.Next() {
			return io.EOF
		}
	} else {
		if !pit.iter.Prev() {
			return io.EOF
		}
	}
	pit.key = copyBytes(pit.iter.Key())
	pit.value = copyBytes(pit.iter.Value())
	return nil
}

// Seek moves the iterator to a new location
func (pit *pebbleIterator) Seek(id []byte) error {
	pit.forward = true
	if !pit.iter.SeekGE(id) {
		return io.EOF
	}
	pit.key = copyBytes(pit.iter.Key())
	pit.value = copyBytes(pit.iter.Value())
	return nil
}

// Seek moves the iterator to a new location
func (pit *pebbleIterator) SeekReverse(id []byte) error {
	pit.forward = false
	if !pit.iter.SeekGE(id) {
		return io.EOF
	}
	if bytes.Compare(id, pit.iter.Key()) < 0 {
		pit.iter.Prev()
	}
	pit.key = copyBytes(pit.iter.Key())
	pit.value = copyBytes(pit.iter.Value())
	return nil
}

// Valid returns true if iterator is still in valid location
func (pit *pebbleIterator) Valid() bool {
	return pit.iter.Valid()
}

// Update runs an alteration transaction of the kvstore. Pebble doesn't
// actually provide transactions, so this is just filling in as a wrapper function
func (pdb *PebbleKV) Update(u func(tx kvi.KVTransaction) error) error {
	ptx := pebbleTransaction{pdb.db}
	return u(ptx)
}

type pebbleBulkWrite struct {
	db    *pebble.DB
	batch *pebble.Batch
	highest, lowest []byte
	curSize int
}

const (
	maxWriterBuffer = 3 << 30
)

func (pbw *pebbleBulkWrite) Set(id []byte, val []byte) error {
	pbw.curSize += len(id) + len(val)
	if pbw.highest == nil || bytes.Compare(id, pbw.highest) > 0 {
		pbw.highest = copyBytes(id)
	}
	if pbw.lowest == nil || bytes.Compare(id, pbw.lowest) < 0 {
		pbw.lowest = copyBytes(id)
	}
	err := pbw.batch.Set(id, val, nil)
	if pbw.curSize > maxWriterBuffer {
		pbw.batch.Commit(nil)
		pbw.batch.Reset()
		pbw.curSize = 0
	}
	return err
}

// BulkWrite is a replication of the regular update, no special code for bulk writes
func (pdb *PebbleKV) BulkWrite(u func(tx kvi.KVBulkWrite) error) error {
	batch := pdb.db.NewBatch()
	ptx := &pebbleBulkWrite{pdb.db, batch, nil, nil, 0}
	err := u(ptx)
	batch.Commit(nil)
	batch.Close()
	if ptx.lowest != nil && ptx.highest != nil {
		pdb.db.Compact(ptx.lowest, ptx.highest)
	}
	return err
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
