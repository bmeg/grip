package grids

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/pebbledb"
	"github.com/bmeg/grip/kvindex"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
)

// Graph implements the GDB interface using a genertic key/value storage driver
type Graph struct {
	graphID  string
	graphKey uint64

	keyMap  *KeyMap
	keykv   pogreb.DB
	graphkv kvi.KVInterface
	indexkv kvi.KVInterface
	idx     *kvindex.KVIndex
	ts      *timestamp.Timestamp
}

// Close the connection
func (g *Graph) Close() error {
	g.keyMap.Close()
	g.graphkv.Close()
	g.indexkv.Close()
	return nil
}

// AddGraph creates a new graph named `graph`
func (kgraph *GDB) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}
	g, err := newGraph(kgraph.basePath, graph)
	if err != nil {
		return err
	}
	kgraph.drivers[graph] = g
	return nil
}

func newGraph(baseDir, name string) (*Graph, error) {
	dbPath := filepath.Join(baseDir, name)

	log.Infof("Creating new GRIDS graph %s", name)

	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		os.Mkdir(dbPath, 0700)
	}
	keykvPath := fmt.Sprintf("%s/keymap", dbPath)
	graphkvPath := fmt.Sprintf("%s/graph", dbPath)
	indexkvPath := fmt.Sprintf("%s/index", dbPath)
	keykv, err := pogreb.Open(keykvPath, nil)
	if err != nil {
		return nil, err
	}
	graphkv, err := pebbledb.NewKVInterface(graphkvPath, kvi.Options{})
	if err != nil {
		return nil, err
	}
	indexkv, err := pebbledb.NewKVInterface(indexkvPath, kvi.Options{})
	if err != nil {
		return nil, err
	}
	ts := timestamp.NewTimestamp()
	o := &Graph{keyMap: NewKeyMap(keykv), graphkv: graphkv, indexkv: indexkv, ts: &ts, idx: kvindex.NewIndex(indexkv)}

	return o, nil
}

// DeleteGraph deletes `graph`
func (kgraph *GDB) DeleteGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return nil
	}
	if d, ok := kgraph.drivers[graph]; ok {
		d.Close()
		delete(kgraph.drivers, graph)
	}
	dbPath := filepath.Join(kgraph.basePath, graph)
	os.RemoveAll(dbPath)
	return nil
}
