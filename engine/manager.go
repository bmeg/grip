package engine

import (
	"io"
	"os"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
)

// NewManager creates a resource manager
func NewManager(workDir string) gdbi.Manager {
	return &manager{[]io.Closer{}, []string{}, workDir}
}

type manager struct {
	kvs     []io.Closer
	paths   []string
	workDir string
}

// GetTmpDir implements gdbi.Manager.
func (bm *manager) GetTmpDir() string {
	td, _ := os.MkdirTemp(bm.workDir, "tmp")
	return td
}

func (bm *manager) GetTempKV() kvi.KVInterface {
	td, _ := os.MkdirTemp(bm.workDir, "kvTmp")
	kv, _ := badgerdb.NewKVInterface(td, kvi.Options{})

	bm.kvs = append(bm.kvs, kv)
	bm.paths = append(bm.paths, td)
	return kv
}

func (bm *manager) Cleanup() {
	for _, c := range bm.kvs {
		c.Close()
	}
	for _, p := range bm.paths {
		os.RemoveAll(p)
	}
}
