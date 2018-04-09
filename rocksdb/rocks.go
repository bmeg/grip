// +build rocks

package rocksdb

import (
	"fmt"
	"log"

	"github.com/bmeg/arachne/kvgraph"
	"github.com/tecbot/gorocksdb"
)

type RocksKV struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

func RocksBuilder(path string) (kvgraph.KVInterface, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(512 * 1024 * 1024))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	log.Printf("Starting RocksDB")
	db, _ := gorocksdb.OpenDb(opts, path)

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	//wo.SetSync(true)

	return &RocksKV{
		db: db,
		ro: ro,
		wo: wo,
	}, nil
}

var Loaded error = kvgraph.AddKVDriver("rocks", RocksBuilder)

//helper function to replicate bytes held in arrays created
//from C pointers in rocks
func bytes_copy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func (self *RocksKV) Close() error {
	self.db.Close()
	return nil
}

func (self *RocksKV) Delete(key []byte) error {
	if err := self.db.Delete(self.wo, key); err != nil {
		return err
	}
	return nil
}

func (self *RocksKV) DeletePrefix(prefix []byte) error {
	del_keys := make([][]byte, 0, 1000)

	it := self.db.NewIterator(self.ro)
	defer it.Close()
	it.Seek(prefix)
	for it = it; it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		okey := bytes_copy(key.Data())
		key.Free()
		del_keys = append(del_keys, okey)
	}
	wb := gorocksdb.NewWriteBatch()
	for _, k := range del_keys {
		wb.Delete(k)
	}
	err := self.db.Write(self.wo, wb)
	if err != nil {
		log.Printf("Del Error: %s", err)
	}
	wb.Destroy()
	return nil
}

func (self *RocksKV) HasKey(key []byte) bool {
	data_value, err := self.db.Get(self.ro, key)
	if err != nil {
		return false
	}
	if data_value.Data() == nil {
		return false
	}
	data_value.Free()
	return true
}

func (self *RocksKV) Set(key []byte, value []byte) error {
	err := self.db.Put(self.wo, key, value)
	return err
}

type RocksTransaction struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

func (self RocksTransaction) Delete(key []byte) error {
	if err := self.db.Delete(self.wo, key); err != nil {
		return err
	}
	return nil
}

func (self *RocksKV) Update(u func(tx kvgraph.KVTransaction) error) error {
	ktx := RocksTransaction{db: self.db, ro: self.ro, wo: self.wo}
	err := u(ktx)
	return err
}

type RocksCursor struct {
	db    *gorocksdb.DB
	ro    *gorocksdb.ReadOptions
	wo    *gorocksdb.WriteOptions
	it    *gorocksdb.Iterator
	key   []byte
	value []byte
}

func (self *RocksCursor) Get(key []byte) ([]byte, error) {
	value, err := self.db.Get(self.ro, key)
	if err != nil {
		return nil, err
	}
	out := bytes_copy(value.Data())
	value.Free()
	return out, nil
}

func (self *RocksCursor) Key() []byte {
	return self.key
}

func (self *RocksCursor) Value() ([]byte, error) {
	return self.value, nil
}

func (self *RocksCursor) Seek(k []byte) error {
	self.it.Seek(k)
	if !self.it.Valid() {
		self.key = nil
		self.value = nil
		return fmt.Errorf("Done")
	}
	key_value := self.it.Key()
	data_value := self.it.Value()
	self.key = bytes_copy(key_value.Data())
	self.value = bytes_copy(data_value.Data())
	key_value.Free()
	data_value.Free()
	return self.it.Err()
}

func (self *RocksCursor) Valid() bool {
	if self.key == nil || self.value == nil {
		return false
	}
	return true
}

func (self *RocksCursor) Next() error {
	self.it.Next()
	if !self.it.Valid() {
		self.key = nil
		self.value = nil
		return fmt.Errorf("Done")
	}
	key_value := self.it.Key()
	data_value := self.it.Value()
	self.key = bytes_copy(key_value.Data())
	self.value = bytes_copy(data_value.Data())
	key_value.Free()
	data_value.Free()
	return nil
}

func (self *RocksKV) View(u func(tx kvgraph.KVIterator) error) error {
	ktx := RocksCursor{db: self.db, ro: self.ro, wo: self.wo, it: self.db.NewIterator(self.ro)}
	err := u(&ktx)
	ktx.it.Close()
	return err
}
