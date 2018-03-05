package kvgraph

import (
	"fmt"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
)

// KVBuilder is function implemented by the various key/value storage drivers
// that returns an initialized KVInterface given a file/path argument
type KVBuilder func(path string) (KVInterface, error)

// KVInterface is the base interface for key/value based graph driver
type KVInterface interface {
	HasKey(key []byte) bool
	Set(key, value []byte) error
	DeletePrefix(prefix []byte) error
	Delete(key []byte) error

	View(func(it KVIterator) error) error
	Update(func(tx KVTransaction) error) error
	Close() error
}

// KVIterator is a genetic interface used by KVInterface.View to allow the
// KVGraph to scan the values stored in the key value driver
type KVIterator interface {
	Seek(k []byte) error
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
	Next() error

	Get(key []byte) ([]byte, error)
}

// KVTransaction is a genetic interface used by KVInterface.Update to allow the
// KVGraph to alter the values stored in the key value driver
type KVTransaction interface {
	Delete(key []byte) error
}

// KVGraph implements the ArachneInterface using a generic key/value storage driver
type KVGraph struct {
	kv KVInterface
	ts *timestamp.Timestamp
}

// KVInterfaceGDB implements the GDB interface using a genertic key/value storage driver
type KVInterfaceGDB struct {
	kv    KVInterface
	graph string
	ts    *timestamp.Timestamp
}

var kvMap = make(map[string]KVBuilder)

// AddKVDriver registers a KeyValue storage driver to the list of avalible drivers.
// Driver list the RocksDB are only included with some build tags and aren't
// always avalible
func AddKVDriver(name string, builder KVBuilder) error {
	kvMap[name] = builder
	return nil
}

// NewKVArachne intitalize a new key value driver give the name of the
// driver and a path/url
func NewKVArachne(name string, path string) (gdbi.GraphDB, error) {
	if x, ok := kvMap[name]; ok {
		kv, err := x(path)
		return NewKVGraph(kv), err
	}
	return nil, fmt.Errorf("Driver %s Not Found", name)
}

// NewKVGraph creats a new instance of KVGraph given a KVInterface
func NewKVGraph(kv KVInterface) gdbi.GraphDB {
	ts := timestamp.NewTimestamp()
	o := &KVGraph{kv: kv, ts: &ts}
	for _, i := range o.GetGraphs() {
		o.ts.Touch(i)
	}
	return o
}
