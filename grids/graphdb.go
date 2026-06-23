package grids

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// GridsGDB implements the GripInterface using a generic key/value storage driver
type GDB struct {
	conf    Config
	drivers map[string]*Graph
	mu      sync.Mutex
}

// NewKVGraphDB intitalize a new grids graph driver
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	conf.SetDefaults()
	log.Redf("Disclaimer: the Grids driver is an experimental database driver. Use with caution.")
	_, err := os.Stat(conf.GraphDir)
	if os.IsNotExist(err) {
		os.Mkdir(conf.GraphDir, 0700)
	}
	return &GDB{conf: conf, drivers: map[string]*Graph{}}, nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (kgraph *GDB) Graph(graph string) (gdbi.GraphInterface, error) {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return nil, err
	}
	kgraph.mu.Lock()
	g, ok := kgraph.drivers[graph]
	kgraph.mu.Unlock()
	if ok {
		return g, nil
	}

	dbPath := filepath.Join(kgraph.conf.GraphDir, graph)
	if _, err := os.Stat(dbPath); err == nil {
		// This also fetches an existing graph if it doesn't exist in kgraph.drivers
		g, err := getGraph(kgraph.conf, graph)
		if err != nil {
			return nil, err
		}
		kgraph.mu.Lock()
		kgraph.drivers[graph] = g
		kgraph.mu.Unlock()

		return g, nil
	}
	return nil, fmt.Errorf("graph '%s' was not found", graph)
}

// ListGraphs lists the graphs managed by this driver
func (gdb *GDB) ListGraphs() []string {
	out := []string{}
	if ds, err := filepath.Glob(filepath.Join(gdb.conf.GraphDir, "*")); err == nil {
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
