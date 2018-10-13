package kvbench

import (
	"log"
	"os"
	"testing"

	"github.com/bmeg/grip/util"

	"github.com/syndtr/goleveldb/leveldb"
	//"github.com/syndtr/goleveldb/leveldb/iterator"
)

func BenchmarkLevelDBPut(b *testing.B) {
	path := "test.level"
	os.RemoveAll(path)
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N*10000; i++ {
		id := util.RandomString(10)
		val := []byte("testing")
		db.Put([]byte(id), val, nil)
	}
	db.Close()
}

func BenchmarkLevelDBBatch(b *testing.B) {
	path := "test.level"
	os.RemoveAll(path)
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := new(leveldb.Batch)
		for j := 0; j < 10000; j++ {
			id := util.RandomString(10)
			val := []byte("testing")
			batch.Put([]byte(id), val)
		}
		err = db.Write(batch, nil)
	}
	db.Close()
}
