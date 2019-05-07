package engine

import (
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/badgerdb"
	log "github.com/sirupsen/logrus"
)

// NewManager creates a resource manager
func NewManager(workDir string) gdbi.Manager {
	return &manager{[]kvi.KVInterface{}, []string{}, workDir}
}

type manager struct {
	kvs     []kvi.KVInterface
	paths   []string
	workDir string
}

func (bm *manager) GetTempKV() kvi.KVInterface {
	_, err := os.Stat(bm.workDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(bm.workDir, 0700)
		if err != nil {
			log.Errorf("GetTempKV: creating work dir: %v", err)
		}
	}

	td, err := ioutil.TempDir(bm.workDir, "kvTmp")
	if err != nil {
		log.Errorf("GetTempKV: creating work dir: %v", err)
	}
	kv, err := badgerdb.NewKVInterface(td, kvi.Options{})
	if err != nil {
		log.Errorf("GetTempKV: creating kvi.KVInterface: %v", err)
	}

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
