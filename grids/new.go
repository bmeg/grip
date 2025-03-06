package grids

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
)

// Graph implements the GDB interface using a genertic key/value storage driver
type Graph struct {
	graphID  string
	graphKey uint64

	keyMap *KeyMap
	keykv  pogreb.DB
	bsonkv *bsontable.BSONDriver
	ts     *timestamp.Timestamp
}

// Close the connection
func (g *Graph) Close() error {
	g.keyMap.Close()
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
	/* todo seperate this out into a new graph func and a get graph func */
	dbPath := filepath.Join(baseDir, name)
	log.Infof("Creating new GRIDS graph %s", name)

	// Create directory if it doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbPath, err)
		}
	}

	keykvPath := fmt.Sprintf("%s/keymap", dbPath)
	keykv, err := pogreb.Open(keykvPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open keykv at %s: %v", keykvPath, err)
	}

	bsonkvPath := fmt.Sprintf("%s/graph", dbPath)
	bsonkv, err := bsontable.NewBSONDriver(bsonkvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bsonkv at %s: %v", bsonkvPath, err)
	}

	ts := timestamp.NewTimestamp()
	o := &Graph{
		keyMap:  NewKeyMap(keykv),
		bsonkv:  bsonkv.(*bsontable.BSONDriver),
		ts:      &ts,
		graphID: name,
	}
	return o, nil
}

func getGraph(baseDir, name string) (*Graph, error) {
	dbPath := filepath.Join(baseDir, name)
	log.Infof("fetching GRIDS graph %s", name)

	keykvPath := fmt.Sprintf("%s/keymap", dbPath)
	keykv, err := pogreb.Open(keykvPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open keykv at %s: %v", keykvPath, err)
	}

	bsonkvPath := fmt.Sprintf("%s/graph", dbPath)
	bsonkv, err := bsontable.LoadBSONDriver(bsonkvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bsonkv at %s: %v", bsonkvPath, err)
	}

	ts := timestamp.NewTimestamp()
	o := &Graph{
		keyMap:  NewKeyMap(keykv),
		bsonkv:  bsonkv.(*bsontable.BSONDriver),
		ts:      &ts,
		graphID: name,
	}
	return o, nil

}

/*
Since each graph has its own directory, delete the directory to delete the graph
*/
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
