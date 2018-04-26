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

// NewKVInterface intitalize a new key value interface given the name of the
// driver and path to create the database
func NewKVInterface(name string, dbPath string) (kvi.KVInterface, error) {
	if builder, ok := kvMap[name]; ok {
		return builder(dbPath)
	}
	return nil, fmt.Errorf("Driver %s Not Found", name)
}

// NewKVGraphDB intitalize a new key value graph driver given the name of the
// driver and path/url to create the database at
func NewKVGraphDB(name string, dbPath string) (gdbi.GraphDB, error) {
	kv, err := NewKVInterface(name, dbPath)
	if err != nil {
		return nil, err
	}
	return NewKVGraph(kv), nil
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
