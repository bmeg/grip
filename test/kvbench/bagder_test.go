package kvbench

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

func badgerInit() *badger.DB {
	path := "test.badger"
	os.RemoveAll(path)
	os.Mkdir(path, 0700)

	opts := badger.Options{}
	opts = badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.SyncWrites = false
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)

	if err != nil {
		log.Printf("Error: %s", err)
	}
	return db
}

func BenchmarkBadgerPut(b *testing.B) {

	db := badgerInit()

	time.Sleep(5 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N*10000; i++ {
		id := randID()
		val := []byte("testing")
		db.Update(func(tx *badger.Txn) error {
			return tx.Set([]byte(id), val)
		})
	}
	db.Close()
}
