// +build rocksdb

/*
The KeyValue interface wrapper for RocksDB
*/

package rocksdb

import (
	"bytes"
	"fmt"

	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

var loaded = kvgraph.AddKVDriver("rocks", NewKVInterface)

// NewKVInterface creates new RocksDB backed KVInterface at `path`
func NewKVInterface(path string) (kvi.KVInterface, error) {
	log.Info("Starting RocksDB")

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(512 * 1024 * 1024))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	//wo.SetSync(true)

	return &RocksKV{
		db: db,
		ro: ro,
		wo: wo,
	}, nil
}

// RocksKV is an implementation of the KVStore for rocksdb
type RocksKV struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

// Close closes the rocksdb connection
func (rockskv *RocksKV) Close() error {
	rockskv.db.Close()
	return nil
}

// Delete removes a key/value from a kvstore
func (rockskv *RocksKV) Delete(key []byte) error {
	return rockskv.db.Delete(rockskv.wo, key)
}

// DeletePrefix deletes all elements in kvstore that begin with prefix `id`
func (rockskv *RocksKV) DeletePrefix(prefix []byte) error {
	delKeys := make([][]byte, 0, 1000)

	it := rockskv.db.NewIterator(rockskv.ro)
	defer it.Close()
	it.Seek(prefix)
	for it := it; it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		okey := copyBytes(key.Data())
		key.Free()
		delKeys = append(delKeys, okey)
	}
	wb := gorocksdb.NewWriteBatch()
	for _, k := range delKeys {
		wb.Delete(k)
	}
	err := rockskv.db.Write(rockskv.wo, wb)
	if err != nil {
		return err
	}
	wb.Destroy()
	return nil
}

// Get retrieves the value of key
func (rockskv *RocksKV) Get(key []byte) ([]byte, error) {
	value, err := rockskv.db.Get(rockskv.ro, key)
	if err != nil {
		return nil, err
	}
	if value.Data() == nil {
		return nil, fmt.Errorf("Not found")
	}
	out := copyBytes(value.Data())
	value.Free()
	return out, nil
}

// HasKey returns true if the key is exists in kvstore
func (rockskv *RocksKV) HasKey(key []byte) bool {
	dataValue, err := rockskv.db.Get(rockskv.ro, key)
	if err != nil {
		return false
	}
	if dataValue.Data() == nil {
		return false
	}
	dataValue.Free()
	return true
}

// Set value in kvstore
func (rockskv *RocksKV) Set(key, value []byte) error {
	return rockskv.db.Put(rockskv.wo, key, value)
}

// Update runs an alteration transaction of the kvstore
func (rockskv *RocksKV) Update(u func(tx kvi.KVTransaction) error) error {
	ktx := rocksTransaction{db: rockskv.db, ro: rockskv.ro, wo: rockskv.wo}
	err := u(ktx)
	return err
}

// View returns an iterator for the kvstore
func (rockskv *RocksKV) View(u func(tx kvi.KVIterator) error) error {
	ktx := &rocksIterator{db: rockskv.db, ro: rockskv.ro, wo: rockskv.wo, it: rockskv.db.NewIterator(rockskv.ro)}
	err := u(ktx)
	ktx.it.Close()
	return err
}

type rocksTransaction struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

func (rocksTxn rocksTransaction) Set(key, value []byte) error {
	return rocksTxn.db.Put(rocksTxn.wo, key, value)
}

func (rocksTxn rocksTransaction) Delete(key []byte) error {
	return rocksTxn.db.Delete(rocksTxn.wo, key)
}

func (rocksTxn rocksTransaction) HasKey(key []byte) bool {
	dataValue, err := rocksTxn.db.Get(rocksTxn.ro, key)
	if err != nil {
		return false
	}
	if dataValue.Data() == nil {
		return false
	}
	dataValue.Free()
	return true
}

func (rocksTxn rocksTransaction) Get(key []byte) ([]byte, error) {
	value, err := rocksTxn.db.Get(rocksTxn.ro, key)
	if err != nil {
		return nil, err
	}
	if value.Data() == nil {
		return nil, fmt.Errorf("Not found")
	}
	out := copyBytes(value.Data())
	value.Free()
	return out, nil
}

type rocksIterator struct {
	db      *gorocksdb.DB
	ro      *gorocksdb.ReadOptions
	wo      *gorocksdb.WriteOptions
	it      *gorocksdb.Iterator
	forward bool
	key     []byte
	value   []byte
}

func (rocksIter *rocksIterator) Get(key []byte) ([]byte, error) {
	value, err := rocksIter.db.Get(rocksIter.ro, key)
	if err != nil {
		return nil, err
	}
	if value.Data() == nil {
		return nil, fmt.Errorf("Not found")
	}
	out := copyBytes(value.Data())
	value.Free()
	return out, nil
}

func (rocksIter *rocksIterator) Key() []byte {
	return rocksIter.key
}

func (rocksIter *rocksIterator) Value() ([]byte, error) {
	return rocksIter.value, nil
}

func (rocksIter *rocksIterator) Seek(k []byte) error {
	rocksIter.it.Seek(k)
	rocksIter.forward = true
	if !rocksIter.it.Valid() {
		rocksIter.key = nil
		rocksIter.value = nil
		return fmt.Errorf("Done")
	}
	keyValue := rocksIter.it.Key()
	dataValue := rocksIter.it.Value()
	rocksIter.key = copyBytes(keyValue.Data())
	rocksIter.value = copyBytes(dataValue.Data())
	keyValue.Free()
	dataValue.Free()
	return rocksIter.it.Err()
}

func (rocksIter *rocksIterator) SeekReverse(k []byte) error {
	rocksIter.it.Seek(k)
	rocksIter.forward = false
	if !rocksIter.it.Valid() {
		rocksIter.key = nil
		rocksIter.value = nil
		return fmt.Errorf("Done")
	}
	keyValue := rocksIter.it.Key()
	//seek lands at value equal or above id. Move once to make sure
	//key is less then id
	if bytes.Compare(k, keyValue.Data()) < 0 {
		keyValue.Free()
		rocksIter.it.Prev()
		keyValue = rocksIter.it.Key()
	}
	dataValue := rocksIter.it.Value()
	rocksIter.key = copyBytes(keyValue.Data())
	rocksIter.value = copyBytes(dataValue.Data())
	keyValue.Free()
	dataValue.Free()
	return rocksIter.it.Err()
}

func (rocksIter *rocksIterator) Valid() bool {
	if rocksIter.key == nil || rocksIter.value == nil {
		return false
	}
	return true
}

func (rocksIter *rocksIterator) Next() error {
	if rocksIter.forward {
		rocksIter.it.Next()
	} else {
		rocksIter.it.Prev()
	}
	if !rocksIter.it.Valid() {
		rocksIter.key = nil
		rocksIter.value = nil
		return fmt.Errorf("Done")
	}
	keyValue := rocksIter.it.Key()
	dataValue := rocksIter.it.Value()
	rocksIter.key = copyBytes(keyValue.Data())
	rocksIter.value = copyBytes(dataValue.Data())
	keyValue.Free()
	dataValue.Free()
	return nil
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
