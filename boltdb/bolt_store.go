package boltdb

import (
	"bytes"
	"fmt"
	"log"
	//"github.com/bmeg/arachne/aql"
	//"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/boltdb/bolt"
)

var GRAPH_BUCKET = []byte("graph")

func BoltBuilder(path string) (kvgraph.KVInterface, error) {
	log.Printf("Starting BOLTDB")
	db, _ := bolt.Open(path, 0600, nil)
	db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(GRAPH_BUCKET) == nil {
			tx.CreateBucket(GRAPH_BUCKET)
		}
		return nil
	})
	return &BoltKV{
		db: db,
	}, nil
}

var Loaded error = kvgraph.AddKVDriver("bolt", BoltBuilder)

type BoltKV struct {
	db *bolt.DB
}

func (self *BoltKV) Close() error {
	return self.db.Close()
}

func (self *BoltKV) Delete(id []byte) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
		return b.Delete(id)
	})
	return err
}

func (self *BoltKV) DeletePrefix(id []byte) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
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

func (self *BoltKV) HasKey(id []byte) bool {
	out := false
	self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
		d := b.Get([]byte(id))
		if d != nil {
			out = true
		}
		return nil
	})
	return out
}

func (self *BoltKV) Set(id []byte, val []byte) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
		b.Put(id, val)
		return nil
	})
	return err
}

type BoltTransaction struct {
	tx *bolt.Tx
	b  *bolt.Bucket
}

func (self BoltTransaction) Delete(id []byte) error {
	return self.b.Delete(id)
}

func (self *BoltKV) Update(u func(tx kvgraph.KVTransaction) error) error {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
		ktx := BoltTransaction{tx, b}
		return u(ktx)
	})
	return err
}

type BoltIterator struct {
	tx    *bolt.Tx
	b     *bolt.Bucket
	c     *bolt.Cursor
	key   []byte
	value []byte
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func (self *BoltIterator) Get(id []byte) ([]byte, error) {
	o := self.b.Get(id)
	if o == nil {
		return nil, fmt.Errorf("Not Found")
	}
	return copyBytes(o), nil
}

func (self *BoltIterator) Key() []byte {
	return self.key
}

func (self *BoltIterator) Value() ([]byte, error) {
	return self.value, nil
}

func (self *BoltIterator) Next() error {
	k, v := self.c.Next()
	if k == nil || v == nil {
		self.key = nil
		self.value = nil
		return nil
	}
	self.key = copyBytes(k)
	self.value = copyBytes(v)
	return nil
}

func (self *BoltIterator) Seek(id []byte) error {
	k, v := self.c.Seek(id)
	if k == nil || v == nil {
		self.key = nil
		self.value = nil
		return fmt.Errorf("Seek error")
	}
	self.key = copyBytes(k)
	self.value = copyBytes(v)
	return nil
}

func (self *BoltIterator) Valid() bool {
	if self.key == nil || self.value == nil {
		return false
	}
	return true
}

func (self *BoltKV) View(u func(it kvgraph.KVIterator) error) error {
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(GRAPH_BUCKET)
		ktx := &BoltIterator{tx, b, b.Cursor(), nil, nil}
		return u(ktx)
	})
	return err
}
