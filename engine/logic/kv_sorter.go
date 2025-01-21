package logic

import (
	"encoding/json"

	"github.com/bmeg/grip/gdbi"
	"github.com/cockroachdb/pebble"
)

const (
	maxWriterBuffer = 3 << 30
)

type KVSorter[T any] struct {
	kv      *pebble.DB
	batch   *pebble.Batch
	curSize int
}

// Close implements gdbi.Sorter.
func (ks *KVSorter[T]) Close() error {
	return ks.kv.Close()
}

// Add implements gdbi.Sorter.
func (ks *KVSorter[T]) Add(key any, value T) {
	if v, err := json.Marshal(value); err == nil {
		k := EncodeAny(key)
		ks.curSize += len(k) + len(v)
		ks.batch.Set(k, v, nil)
		if ks.curSize > maxWriterBuffer {
			ks.batch.Commit(nil)
			ks.batch.Reset()
			ks.curSize = 0
		}
	}
}

// Sorted implements gdbi.Sorter.
func (ks *KVSorter[T]) Sorted() chan T {
	ks.batch.Commit(nil)
	ks.batch.Close()

	out := make(chan T)
	go func() {
		iter, _ := ks.kv.NewIter(nil)
		for iter.First(); iter.Valid(); iter.Next() {
			v := iter.Value()
			var o T
			json.Unmarshal(v, &o)
			out <- o
		}
		defer close(out)
	}()
	return out
}

func NewKVSorter[T any](path string) gdbi.Sorter[T] {
	c := *pebble.DefaultComparer
	c.Compare = CompareEncoded
	kv, _ := pebble.Open(path, &pebble.Options{Comparer: &c})
	b := kv.NewBatch()
	return &KVSorter[T]{kv, b, 0}
}
