package grids

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvi/pebbledb"
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
	bsonkv  *bsontable.BSONDriver
	ts      *timestamp.Timestamp
}

// Close the connection
func (g *Graph) Close() error {
	g.keyMap.Close()
	g.graphkv.Close()
	g.bsonkv.Close()
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

	// Create directory if it doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbPath, err)
		}
	}

	// Open resources with cleanup on failure
	keykvPath := fmt.Sprintf("%s/keymap", dbPath)
	keykv, err := pogreb.Open(keykvPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open keykv at %s: %v", keykvPath, err)
	}
	defer func() {
		if err != nil {
			keykv.Close()
		}
	}()

	graphkvPath := fmt.Sprintf("%s/graph", dbPath)
	graphkv, err := pebbledb.NewKVInterface(graphkvPath, kvi.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open graphkv at %s: %v", graphkvPath, err)
	}
	defer func() {
		if err != nil {
			graphkv.Close()
		}
	}()

	bsonkvPath := fmt.Sprintf("%s/index", dbPath)
	bsonkv, err := bsontable.NewBSONDriver(bsonkvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bsonkv at %s: %v", bsonkvPath, err)
	}

	// All resources opened successfully, construct the Graph
	ts := timestamp.NewTimestamp()
	o := &Graph{
		keyMap:  NewKeyMap(keykv),
		graphkv: graphkv,
		bsonkv:  bsonkv.(*bsontable.BSONDriver),
		ts:      &ts,
	}
	log.Info("WE MADE IT HERE")
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
