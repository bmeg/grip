package grids

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// GridsGDB implements the GripInterface using a generic key/value storage driver
type GDB struct {
	basePath string
	drivers  map[string]*Graph
}

// NewKVGraphDB intitalize a new grids graph driver
func NewGraphDB(baseDir string) (gdbi.GraphDB, error) {
	log.Warning("GRIP driver is development. Do not use")
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
	if g, ok := kgraph.drivers[graph]; ok {
		return g, nil
	}
	dbPath := filepath.Join(kgraph.basePath, graph)
	if _, err := os.Stat(dbPath); err == nil {
		g, err := newGraph(kgraph.basePath, graph)
		if err != nil {
			return nil, err
		}
		kgraph.drivers[graph] = g
		return g, nil
	}
	return nil, fmt.Errorf("graph '%s' was not found", graph)
}

// ListGraphs lists the graphs managed by this driver
func (gdb *GDB) ListGraphs() []string {
	out := []string{}
	for k := range gdb.drivers {
		out = append(out, k)
	}
	if ds, err := filepath.Glob(filepath.Join(gdb.basePath, "*")); err == nil {
		for _, d := range ds {
			b := filepath.Base(d)
			out = append(out, b)
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
