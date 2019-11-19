package benchmark

import (
	//"fmt"
	"encoding/binary"
	"math/rand"
	"os"
	"testing"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
	log "github.com/sirupsen/logrus"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

var testDBSize = 100000

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func BenchmarkStringInsert(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}
	b.Run("insert-string", func(b *testing.B) {
		keys := [][]byte{}
		for i := 0; i < b.N; i++ {
			s := RandStringRunes(20)
			keys = append(keys, []byte(s))
		}
		b.ResetTimer()
		db.Update(func(tx kvi.KVTransaction) error {
			for i := 0; i < b.N; i++ {
				tx.Set(keys[i], []byte{0})
			}
			return nil
		})
	})
	db.Close()
	os.RemoveAll("test.db")
}

func BenchmarkIntInsert(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}

	b.Run("insert-int", func(b *testing.B) {
		keys := [][]byte{}
		buf := make([]byte, binary.MaxVarintLen64)
		for i := 0; i < b.N; i++ {
			s := rand.Uint64()
			binary.PutUvarint(buf, s)
			keys = append(keys, buf)
		}
		b.ResetTimer()
		db.Update(func(tx kvi.KVTransaction) error {
			for i := 0; i < b.N; i++ {
				tx.Set(keys[i], []byte{0})
			}
			return nil
		})
	})
	db.Close()
	os.RemoveAll("test.db")
}

func BenchmarkHashInsert(b *testing.B) {
	os.Mkdir("test_idx.db", 0700)
	dbIdx, err := pogreb.Open("test_idx.db/keys", nil)
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}
	b.Run("insert-hash", func(b *testing.B) {
		keys := [][]byte{}
		for i := 0; i < b.N; i++ {
			s := RandStringRunes(20)
			keys = append(keys, []byte(s))
		}
		b.ResetTimer()
		//db.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var idx uint64
		buf := make([]byte, binary.MaxVarintLen64)
		for i := 0; i < b.N; i++ {
			binary.PutUvarint(buf, idx)
			dbIdx.Put(keys[i], buf)
			idx++
		}
	})
	dbIdx.Close()
	os.RemoveAll("test_idx.db")
}

func BenchmarkMixedInsert(b *testing.B) {
	os.Mkdir("test_idx.db", 0700)
	dbIdx, err := pogreb.Open("test_idx.db/keys", nil)
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}

	b.Run("insert-mixed", func(b *testing.B) {
		keys := [][]byte{}
		for i := 0; i < b.N; i++ {
			s := RandStringRunes(20)
			keys = append(keys, []byte(s))
		}
		b.ResetTimer()
		//db.BulkWrite(func(tx kvi.KVBulkWrite) error {
		var idx uint64
		buf := make([]byte, binary.MaxVarintLen64)
		db.Update(func(tx kvi.KVTransaction) error {
			for i := 0; i < b.N; i++ {
				binary.PutUvarint(buf, idx)
				dbIdx.Put(keys[i], buf)
				tx.Set(buf, []byte{0})
				idx++
			}
			return nil
		})
	})
	db.Close()
	dbIdx.Close()
	os.RemoveAll("test.db")
	os.RemoveAll("test_idx.db")
}

func BenchmarkStringScan(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}
	keys := [][]byte{}
	db.Update(func(tx kvi.KVTransaction) error {
		for i := 0; i < testDBSize; i++ {
			s := RandStringRunes(20)
			keys = append(keys, []byte(s))
			tx.Set([]byte(s), []byte{0})
		}
		return nil
	})
	b.Run("string-scan", func(b *testing.B) {
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				for it.Seek([]byte{0x00}); it.Valid(); it.Next() {
				}
			}
			return nil
		})
	})

	b.Run("string-get", func(b *testing.B) {
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				it.Get(keys[rand.Intn(len(keys))])
			}
			return nil
		})
	})

	db.Close()
	os.RemoveAll("test.db")
}

func BenchmarkIntScan(b *testing.B) {
	db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
	if err != nil {
		log.Errorf("issue: %s", err)
		return
	}
	keys := [][]byte{}
	buf := make([]byte, binary.MaxVarintLen64)
	db.Update(func(tx kvi.KVTransaction) error {
		for i := 0; i < testDBSize; i++ {
			s := rand.Uint64()
			binary.PutUvarint(buf, s)
			keys = append(keys, buf)
			tx.Set(buf, []byte{0})
		}
		return nil
	})
	b.Run("int-scan", func(b *testing.B) {
		b.ResetTimer()
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				for it.Seek([]byte{0x00}); it.Valid(); it.Next() {
				}
			}
			return nil
		})
	})

	b.Run("int-get", func(b *testing.B) {
		db.View(func(it kvi.KVIterator) error {
			for i := 0; i < b.N; i++ {
				it.Get(keys[rand.Intn(len(keys))])
			}
			return nil
		})
	})

	b.Run("int-get-multiview", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.View(func(it kvi.KVIterator) error {
				it.Get(keys[rand.Intn(len(keys))])
				return nil
			})
		}
	})

	db.Close()
	os.RemoveAll("test.db")
}
