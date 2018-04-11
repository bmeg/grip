package kvgraph

import (
	"fmt"

	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvi"
	"github.com/bmeg/arachne/kvindex"
	"github.com/bmeg/arachne/timestamp"
)

// KVGraph implements the ArachneInterface using a generic key/value storage driver
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

var kvMap = make(map[string]kvi.KVBuilder)

// AddKVDriver registers a KeyValue storage driver to the list of avalible drivers.
// Driver list the RocksDB are only included with some build tags and aren't
// always avalible
func AddKVDriver(name string, builder kvi.KVBuilder) error {
	kvMap[name] = builder
	return nil
}

// NewKVGraphDB intitalize a new key value driver give the name of the
// driver and a path/url
func NewKVGraphDB(name string, path string) (gdbi.GraphDB, error) {
	if x, ok := kvMap[name]; ok {
		kv, err := x(path)
		if err != nil {
			return nil, err
		}
		return NewKVGraph(kv), nil
	}
	return nil, fmt.Errorf("Driver %s Not Found", name)
}

// NewKVGraph creats a new instance of KVGraph given a KVInterface
func NewKVGraph(kv kvi.KVInterface) gdbi.GraphDB {
	ts := timestamp.NewTimestamp()
	o := &KVGraph{kv: kv, ts: &ts, idx: kvindex.NewIndex(kv)}
	for _, i := range o.GetGraphs() {
		o.ts.Touch(i)
	}
	return o
}
