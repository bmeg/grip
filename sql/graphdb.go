package sql

import (
	"errors"
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/jmoiron/sqlx"
)

// Vertex describes the mapping of a table to the graph
type Vertex struct {
	Table    string
	GidField string
	Label    string
}

// Edge describes the mapping between two tables.
// It may also describe a relational table containing edge properties.
type Edge struct {
	Table    string
	GidField string
	Label    string
	From     *ForeignKey
	To       *ForeignKey
}

// ForeignKey describes a relation to another table
type ForeignKey struct {
	SourceField string
	DestTable   string
	DestField   string
}

// Schema describes the mapping of tables to the graph.
type Schema struct {
	Graph    string
	Vertices []*Vertex
	Edges    []*Edge
}

// Config describes the configuration for the sql driver.
type Config struct {
	// the driver-specific data source name, usually consisting of at least
	// a database name and connection information
	DataSourceName string
	// The driver name ("mysql", "postgres", etc)
	Driver string
	// The graph definitions
	Graphs []*Schema
}

// GraphDB manages graphs in the database
type GraphDB struct {
	db     *sqlx.DB
	graphs []*Schema
	ts     *timestamp.Timestamp
}

// NewGraphDB creates a new GraphDB graph database interface
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	for _, s := range conf.Graphs {
		err := ValidateSchema(s)
		if err != nil {
			return nil, fmt.Errorf("schema validation failed for graph %s: %v", s.Graph, err)
		}
	}
	db, err := sqlx.Connect(conf.Driver, conf.DataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(5)
	ts := timestamp.NewTimestamp()
	gdb := &GraphDB{db, conf.Graphs, &ts}
	for _, i := range gdb.ListGraphs() {
		gdb.ts.Touch(i)
	}
	return gdb, nil
}

// Close the connection
func (db *GraphDB) Close() error {
	return db.db.Close()
}

// AddGraph creates a new graph named `graph`
func (db *GraphDB) AddGraph(graph string) error {
	return errors.New("not implemented")
}

// DeleteGraph deletes an existing graph named `graph`
func (db *GraphDB) DeleteGraph(graph string) error {
	return errors.New("not implemented")
}

// ListGraphs lists the graphs managed by this driver
func (db *GraphDB) ListGraphs() []string {
	out := []string{}
	for _, schema := range db.graphs {
		out = append(out, schema.Graph)
	}
	return out
}

func (db *GraphDB) getGraphSchema(graph string) *Schema {
	for _, schema := range db.graphs {
		if schema.Graph == graph {
			return schema
		}
	}
	return nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (db *GraphDB) Graph(graph string) (gdbi.GraphInterface, error) {
	schema := db.getGraphSchema(graph)
	if schema == nil {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &Graph{
		db:     db.db,
		ts:     db.ts,
		graph:  graph,
		schema: schema,
	}, nil
}

// GetSchema returns the schema of a specific graph in the database
func (db *GraphDB) GetSchema(graph string, sampleN uint32) (*aql.GraphSchema, error) {
	return nil, fmt.Errorf("not implemented")
}
