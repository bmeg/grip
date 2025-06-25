package grids

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
)

// GridsGDB implements the GripInterface using a generic key/value storage driver
type GDB struct {
	basePath string
	drivers  map[string]*Graph
}

// NewKVGraphDB intitalize a new grids graph driver
func NewGraphDB(baseDir string) (gdbi.GraphDB, error) {
	_, err := os.Stat(baseDir)
	if os.IsNotExist(err) {
		os.Mkdir(baseDir, 0700)
	}
	return &GDB{basePath: baseDir, drivers: map[string]*Graph{}}, nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *GDB) Graph(graph string) (gdbi.GraphInterface, error) {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return nil, err
	}
	mu := sync.Mutex{}
	mu.Lock()
	g, ok := kgraph.drivers[graph]
	mu.Unlock()
	if ok {
		return g, nil
	}

	dbPath := filepath.Join(kgraph.basePath, graph)
	if _, err := os.Stat(dbPath); err == nil {
		// This also fetches an existing graph if it doesn't exist in kgraph.drivers
		g, err := getGraph(kgraph.basePath, graph)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		kgraph.drivers[graph] = g
		mu.Unlock()

		return g, nil
	}
	return nil, fmt.Errorf("graph '%s' was not found", graph)
}

// ListGraphs lists the graphs managed by this driver
func (gdb *GDB) ListGraphs() []string {
	out := []string{}
	if ds, err := filepath.Glob(filepath.Join(gdb.basePath, "*")); err == nil {
		for _, d := range ds {
			fi, err := os.Stat(d)
			if err != nil {
				continue
			}
			if fi.IsDir() {
				versionPath := filepath.Join(d, "VERSION")
				if _, err := os.Stat(versionPath); err == nil {
					b := filepath.Base(d)
					out = append(out, b)
				}
			}
		}
	}
	return out
}

// Close the graphs
func (kgraph *GDB) Close() error {
	for _, g := range kgraph.drivers {
		g.Close()
	}
	return nil
}
