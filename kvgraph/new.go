package kvgraph

import (
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/timestamp"
)

// KVGraph implements the GripInterface using a generic key/value storage driver
type KVGraph struct {
	kv  kvi.KVInterface
	idx *kvindex.KVIndex
	ts  *timestamp.Timestamp
}

// KVInterfaceGDB implements the GDB interface using a genertic key/value storage driver
type KVInterfaceGDB struct {
	kvg   *KVGraph
	graph string
}

// NewKVGraphDB intitalize a new key value graph driver given the name of the
// driver and path/url to create the database at
func NewKVGraphDB(name string, dbPath string) (gdbi.GraphDB, error) {
	kv, err := kvi.NewKVInterface(name, dbPath, nil)
	if err != nil {
		return nil, err
	}
	return NewKVGraph(kv), nil
}

// NewKVGraph creats a new instance of KVGraph given a KVInterface
func NewKVGraph(kv kvi.KVInterface) gdbi.GraphDB {
	ts := timestamp.NewTimestamp()
	o := &KVGraph{kv: kv, ts: &ts, idx: kvindex.NewIndex(kv)}
	for _, i := range o.ListGraphs() {
		o.ts.Touch(i)
	}
	return o
}
