package logic

import (
	"github.com/bmeg/grip/log"
	"github.com/cockroachdb/pebble"
)

const (
	maxWriterBuffer = 3 << 30
)

type kvCompare[T any] struct {
	conf SortConf[T]
}

type KVSorter[T any] struct {
	kv      *pebble.DB
	batch   *pebble.Batch
	curSize int
	compare kvCompare[T]
}

// Close implements gdbi.Sorter.
func (ks *KVSorter[T]) Close() error {
	return ks.kv.Close()
}

// Add implements gdbi.Sorter.
func (ks *KVSorter[T]) Add(value T) {

	k := ks.compare.conf.ToBytes(value)
	ks.curSize += len(k)
	ks.batch.Set(k, nil, nil)
	if ks.curSize > maxWriterBuffer {
		ks.batch.Commit(nil)
		ks.batch.Reset()
		ks.curSize = 0
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
			v := iter.Key()
			var o T
			o, err := ks.compare.conf.FromBytes(v)
			if err != nil {
				log.Infof("error in Sorted: %s\n", err)
			}
			out <- o
		}
		defer close(out)
	}()
	return out
}

func (ks *kvCompare[T]) compareEncoded(a, b []byte) int {
	aT, err := ks.conf.FromBytes(a)
	if err != nil {
		log.Debugf("error compareEncoded: %s\n", err)
	}
	bT, err := ks.conf.FromBytes(b)
	if err != nil {
		log.Debugf("error compareEncoded: %s\n", err)
	}
	return ks.conf.Compare(aT, bT)
}

func NewKVSorter[T any](path string, conf SortConf[T]) Sorter[T] {
	comp := kvCompare[T]{conf}
	c := *pebble.DefaultComparer
	c.Compare = comp.compareEncoded
	o := &KVSorter[T]{}
	o.kv, _ = pebble.Open(path, &pebble.Options{Comparer: &c})
	o.batch = o.kv.NewBatch()
	o.curSize = 0
	o.compare = comp
	return o
}
