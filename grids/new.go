package grids

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bmeg/grip/grids/driver"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
)

// Graph implements the GDB interface using a genertic key/value storage driver
type Graph struct {
	graphID string

	driver            *driver.GridKVDriver
	ts                *timestamp.Timestamp
	tempDeletedEdges  map[string]struct{}
	edgesMutex        sync.Mutex
	BulkLoaderWorkers int
}

// Close the connection
func (g *Graph) Close() error {
	g.driver.Close()
	return nil
}

// AddGraph creates a new graph named `graph`
func (kgraph *GDB) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}
	g, err := newGraph(kgraph.conf, graph)
	if err != nil {
		return err
	}
	kgraph.mu.Lock()
	defer kgraph.mu.Unlock()
	kgraph.drivers[graph] = g
	return nil
}

func newGraph(conf Config, name string) (*Graph, error) {
	dbPath := filepath.Join(conf.GraphDir, name)
	fmt.Printf("Creating new GRIDS graph %s\n", name)

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbPath, err)
		}
	}

	versionPath := filepath.Join(dbPath, "VERSION")
	if err := os.WriteFile(versionPath, []byte("0.0.1"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create VERSION file: %v", err)
	}

	drvr, err := driver.NewGridKVDriver(dbPath, conf.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to open grids storage at %s: %v", dbPath, err)
	}

	ts := timestamp.NewTimestamp()

	o := &Graph{
		driver:            drvr,
		ts:                &ts,
		graphID:           name,
		tempDeletedEdges:  make(map[string]struct{}),
		edgesMutex:        sync.Mutex{},
		BulkLoaderWorkers: conf.BulkLoaderWorkers,
	}
	return o, nil
}

func getGraph(conf Config, name string) (*Graph, error) {
	dbPath := filepath.Join(conf.GraphDir, name)
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
			return nil, fmt.Errorf("unsupported version %s", version)
		}
	}

	drvr, err := driver.NewGridKVDriver(dbPath, conf.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to open grids storage at %s: %v", dbPath, err)
	}

	ts := timestamp.NewTimestamp()

	o := &Graph{
		driver:            drvr,
		ts:                &ts,
		graphID:           name,
		tempDeletedEdges:  make(map[string]struct{}),
		edgesMutex:        sync.Mutex{},
		BulkLoaderWorkers: conf.BulkLoaderWorkers,
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
	kgraph.mu.Lock()
	defer kgraph.mu.Unlock()
	if d, ok := kgraph.drivers[graph]; ok {
		d.Close()
		delete(kgraph.drivers, graph)
	}
	dbPath := filepath.Join(kgraph.conf.GraphDir, graph)
	os.RemoveAll(dbPath)
	return nil
}
