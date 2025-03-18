package grids

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
)

// Graph implements the GDB interface using a genertic key/value storage driver
type Graph struct {
	graphID string

	keyMap *KeyMap
	bsonkv *bsontable.BSONDriver
	ts     *timestamp.Timestamp
}

// Close the connection
func (g *Graph) Close() error {
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
	fmt.Printf("Creating new GRIDS graph %s\n", name)

	// Create directory if it doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbPath, err)
		}
	}

	// Create VERSION file
	versionPath := filepath.Join(dbPath, "VERSION")
	if err := os.WriteFile(versionPath, []byte("0.0.1"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create VERSION file: %v", err)
	}

	//bsonkvPath := fmt.Sprintf("%s", dbPath)
	bsonkvPath := dbPath
	tabledr, err := bsontable.NewBSONDriver(bsonkvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bsonkv at %s: %v", bsonkvPath, err)
	}
	bsonkv := tabledr.(*bsontable.BSONDriver)

	ts := timestamp.NewTimestamp()

	o := &Graph{
		keyMap:  NewKeyMap(),
		bsonkv:  bsonkv,
		ts:      &ts,
		graphID: name,
	}
	return o, nil
}

func getGraph(baseDir, name string) (*Graph, error) {
	dbPath := filepath.Join(baseDir, name)
	fmt.Printf("fetching GRIDS graph %s\n", name)

	versionPath := filepath.Join(dbPath, "VERSION")
	file, err := os.Open(versionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open VERSION file at %s: %v", versionPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		version := scanner.Text()
		if strings.TrimSpace(version) != "0.0.1" {
			return nil, fmt.Errorf("VERSION file at %s does not have '0.0.1' on the first line", versionPath)
		}
	} else {
		return nil, fmt.Errorf("VERSION file at %s is empty", versionPath)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading VERSION file at %s: %v", versionPath, err)
	}

	//bsonkvPath := fmt.Sprintf("%s", dbPath)
	bsonkvPath := dbPath
	tabledr, err := bsontable.LoadBSONDriver(bsonkvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bsonkv at %s: %v", bsonkvPath, err)
	}

	bsonkv := tabledr.(*bsontable.BSONDriver)

	ts := timestamp.NewTimestamp()
	o := &Graph{
		keyMap:  NewKeyMap(),
		bsonkv:  bsonkv,
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
