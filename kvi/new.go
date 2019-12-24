package kvi

import (
	"fmt"
)

var kvMap = make(map[string]KVBuilder)

// AddKVDriver registers a KeyValue storage driver to the list of avalible drivers.
// Driver list the RocksDB are only included with some build tags and aren't
// always avalible
func AddKVDriver(name string, builder KVBuilder) error {
	kvMap[name] = builder
	return nil
}

// NewKVInterface intitalize a new key value interface given the name of the
// driver and path to create the database
func NewKVInterface(name string, dbPath string, opts *Options) (KVInterface, error) {
	if builder, ok := kvMap[name]; ok {
		if opts != nil {
			return builder(dbPath, *opts)
		}
		return builder(dbPath, Options{})
	}
	return nil, fmt.Errorf("driver %s Not Found", name)
}
