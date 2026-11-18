package table

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/schema"
	"github.com/bmeg/grip/gripql"
)

// GraphConfig declares one named graph backed by a table driver.
type GraphConfig struct {
	Graph   string  `json:"graph"`
	Mapping Mapping `json:"mapping"`
}

// Config configures a standalone table GraphDB.
type Config struct {
	// Driver is the name of a registered TableDriver factory.
	Driver string `json:"driver,omitempty"`
	// Plugin is an optional path to a -buildmode=plugin dynamic table module.
	Plugin string `json:"plugin,omitempty"`
	// Config is passed to the registered factory.
	Config map[string]interface{} `json:"config,omitempty"`
	Graphs []GraphConfig          `json:"graphs"`
}

// Factory creates a table driver for a server configuration.
type Factory func(context.Context, map[string]interface{}) (TableDriver, error)

var factoryMu sync.RWMutex
var factories = map[string]Factory{}

// RegisterFactory registers a named table-driver factory. The name is used by
// table.Config.Driver and must be unique within the process.
func RegisterFactory(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("%w: table driver name is empty", ErrInvalidRequest)
	}
	if factory == nil {
		return fmt.Errorf("%w: table driver factory is nil", ErrInvalidRequest)
	}
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if _, exists := factories[name]; exists {
		return fmt.Errorf("%w: table driver %q is already registered", ErrInvalidRequest, name)
	}
	factories[name] = factory
	return nil
}

// NewConfiguredGraphDB creates a standalone GraphDB using a registered factory.
func NewConfiguredGraphDB(ctx context.Context, config Config) (gdbi.GraphDB, error) {
	if config.Plugin != "" {
		loaded, err := LoadPlugin(config.Plugin, config.Config)
		if err != nil {
			return nil, fmt.Errorf("loading table plugin %q: %w", config.Plugin, err)
		}
		db, err := NewGraphDB(ctx, loaded.Driver, config)
		if err != nil {
			_ = loaded.Close()
			return nil, err
		}
		tableDB := db.(*GraphDB)
		tableDB.plugin = loaded
		return tableDB, nil
	}
	if config.Driver == "" {
		return nil, fmt.Errorf("%w: table driver name is empty", ErrInvalidRequest)
	}
	factoryMu.RLock()
	factory, ok := factories[config.Driver]
	factoryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: table driver %q is not registered", ErrInvalidRequest, config.Driver)
	}
	driver, err := factory(ctx, config.Config)
	if err != nil {
		return nil, fmt.Errorf("starting table driver %q: %w", config.Driver, err)
	}
	return NewGraphDB(ctx, driver, config)
}

// GraphDB manages named graphs backed by one table driver.
type GraphDB struct {
	driver TableDriver
	graphs map[string]*Graph
	config Config
	plugin *LoadedPlugin
}

// NewGraphDB creates a standalone table GraphDB.
func NewGraphDB(ctx context.Context, driver TableDriver, config Config) (gdbi.GraphDB, error) {
	if driver == nil {
		return nil, fmt.Errorf("%w: table driver is nil", ErrInvalidRequest)
	}
	if len(config.Graphs) == 0 {
		return nil, fmt.Errorf("%w: table GraphDB has no graphs", ErrInvalidRequest)
	}
	db := &GraphDB{
		driver: driver,
		graphs: map[string]*Graph{},
		config: config,
	}
	seen := map[string]bool{}
	for _, graphConfig := range config.Graphs {
		if err := gripql.ValidateGraphName(graphConfig.Graph); err != nil {
			return nil, fmt.Errorf("invalid table graph name %q: %w", graphConfig.Graph, err)
		}
		if seen[graphConfig.Graph] {
			return nil, fmt.Errorf("%w: duplicate table graph %q", ErrInvalidRequest, graphConfig.Graph)
		}
		seen[graphConfig.Graph] = true
	}
	for _, graphConfig := range config.Graphs {
		graph, err := NewGraph(ctx, driver, graphConfig.Mapping)
		if err != nil {
			return nil, fmt.Errorf("building table graph %q: %w", graphConfig.Graph, err)
		}
		db.graphs[graphConfig.Graph] = graph
	}
	return db, nil
}

func (db *GraphDB) AddGraph(graph string) error {
	if _, exists := db.graphs[graph]; exists {
		return nil
	}
	return fmt.Errorf("%w: table graphs must be declared in configuration", ErrUnsupported)
}

func (db *GraphDB) DeleteGraph(graph string) error {
	if _, exists := db.graphs[graph]; !exists {
		return fmt.Errorf("%w: graph %q", ErrCollectionNotFound, graph)
	}
	return fmt.Errorf("%w: deleting table graphs", ErrUnsupported)
}

func (db *GraphDB) ListGraphs() []string {
	names := make([]string, 0, len(db.graphs))
	for name := range db.graphs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (db *GraphDB) Graph(graph string) (gdbi.GraphInterface, error) {
	g, ok := db.graphs[graph]
	if !ok {
		return nil, fmt.Errorf("table graph %q was not found", graph)
	}
	return g, nil
}

func (db *GraphDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
	g, err := db.Graph(graph)
	if err != nil {
		return nil, err
	}
	return schema.SchemaScan(ctx, graph, g, sampleN, random)
}

func (db *GraphDB) Close() error {
	if db.plugin != nil {
		return db.plugin.Close()
	}
	if closer, ok := db.driver.(CloseTableDriver); ok {
		return closer.Close()
	}
	return nil
}
