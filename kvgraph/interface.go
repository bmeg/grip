package kvgraph

import (
	"fmt"
	"github.com/bmeg/arachne/gdbi"
)

type KVBuilder func(path string) (KVInterface, error)

type KVInterface interface {
	HasKey(key []byte) bool
	Set(key, value []byte) error
	DeletePrefix(prefix []byte) error
	Delete(key []byte) error

	View(func(it KVIterator) error) error
	Update(func(tx KVTransaction) error) error
	Close() error
}

type KVIterator interface {
	Seek(k []byte) error
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
	Next() error

	Get(key []byte) ([]byte, error)
}

type KVTransaction interface {
	Delete(key []byte) error
}

type KVGraph struct {
	kv KVInterface
}

type KVInterfaceGDB struct {
	kv    KVInterface
	graph string
}

var kvMap map[string]KVBuilder = make(map[string]KVBuilder)

func AddKVDriver(name string, builder KVBuilder) error {
	kvMap[name] = builder
	return nil
}

func NewKVArachne(name string, path string) (gdbi.ArachneInterface, error) {
	if x, ok := kvMap[name]; ok {
		kv, err := x(path)
		return &KVGraph{kv: kv}, err
	}
	return nil, fmt.Errorf("Driver %s Not Found", name)
}

func NewKVGraph(kv KVInterface) gdbi.ArachneInterface {
	return &KVGraph{kv: kv}
}
