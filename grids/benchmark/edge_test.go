package benchmark

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
)

func BenchmarkEdgeStringScan(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}

	keys := make([]string, 0, keySetSize)
	for i := 0; i < keySetSize; i++ {
		k := RandStringRunes(10)
		keys = append(keys, k)
	}

	for i := 0; i < 1000; i++ {
		db.Update(func(tx kvi.KVTransaction) error {
			for i := 0; i < 4; i++ {
				id := util.UUID()
				src := keys[rand.Intn(len(keys))]
				dst := keys[rand.Intn(len(keys))]
				label := "test"
				k := srcEdgeStringKey("graph", src, dst, id, label)
				tx.Set(k, []byte{0})
			}
			return nil
		})
	}

	b.Run("edge-scan-string", func(b *testing.B) {
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				skeyPrefix := srcEdgeStringKeyPrefix("graph", keys[rand.Intn(len(keys))])
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					srcEdgeStringKeyParse(keyValue)
				}
			}
			return nil
		})
	})

	db.Close()
	os.RemoveAll("test.db")
}

func BenchmarkEdgeIntScan(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}

	keys := make([]uint64, 0, keySetSize)
	for i := 0; i < keySetSize; i++ {
		k := rand.Uint64()
		keys = append(keys, k)
	}

	var idx uint64
	for i := 0; i < 1000; i++ {
		db.Update(func(tx kvi.KVTransaction) error {
			for i := 0; i < 4; i++ {
				id := idx
				src := keys[rand.Intn(len(keys))]
				dst := keys[rand.Intn(len(keys))]
				label := uint64(3)
				k := srcEdgeIntKey(1, src, dst, id, label)
				tx.Set(k, []byte{0})
				idx++
			}
			return nil
		})
	}

	b.Run("edge-scan-int", func(b *testing.B) {
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				skeyPrefix := srcEdgeIntKeyPrefix(1, keys[rand.Intn(len(keys))])
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					keyValue := it.Key()
					srcEdgeIntKeyParse(keyValue)
				}
			}
			return nil
		})
	})

	db.Close()
	os.RemoveAll("test.db")
}
