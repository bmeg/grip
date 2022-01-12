package grids

import (
	"fmt"

	"github.com/bmeg/grip/gdbi"
)

// GridsGDB implements the GripInterface using a generic key/value storage driver
type GDB struct {
	basePath string
	drivers  map[string]*Graph
}

// NewKVGraphDB intitalize a new grids graph driver
func NewGraphDB(baseDir string) (gdbi.GraphDB, error) {
	return &GDB{basePath: baseDir, drivers: map[string]*Graph{}}, nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *GDB) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range kgraph.ListGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &Graph{kdb: kgraph, graphID: graph}, nil
}

// ListGraphs lists the graphs managed by this driver
func (kgraph *GDB) ListGraphs() []string {
	out := []string{}

	return out
}

// Close the graphs
func (kgraph *GDB) Close() error {
	for _, g := range kgraph.drivers {
		g.Close()
	}
	return nil
}
