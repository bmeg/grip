package engine

import (
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/kvgraph"
	"io/ioutil"
	"os"
)

func (pipe Pipeline) NewManager() Manager {
	return &badgerManager{[]kvgraph.KVInterface{}, []string{}, pipe.workDir}
}

type badgerManager struct {
	kvs     []kvgraph.KVInterface
	paths   []string
	workDir string
}

func (bm *badgerManager) GetTempKV() kvgraph.KVInterface {
	td, _ := ioutil.TempDir(bm.workDir, "kvTmp")
	kv, _ := badgerdb.BadgerBuilder(td)

	bm.kvs = append(bm.kvs, kv)
	bm.paths = append(bm.paths, td)
	return kv
}

func (bm *badgerManager) Cleanup() {
	for _, c := range bm.kvs {
		c.Close()
	}
	for _, p := range bm.paths {
		os.RemoveAll(p)
	}
}
