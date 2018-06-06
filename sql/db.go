package sql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
)

// Vertex describes the mapping of a table to the graph
type Vertex struct {
	Table string
	Gid   string
	Label string
}

// Edge describes the mapping between two tables.
// It may also describe a relational table containing edge properties.
type Edge struct {
	Table string
	Gid   string
	Label string
	From  Vertex
	To    Vertex
}

// Schema describes the mapping of tables to the graph.
type Schema struct {
	Vertices []*Vertex
	Edges    []*Edge
}

// Config describes the configuration for the sql driver.
type Config struct {
	URL    string
	Driver string
	Graphs map[string]*Schema
}

// SQL is the base driver that manages multiple graphs in a sql databases
type SQL struct {
	db     *sql.DB
	graphs map[string]*Schema
	ts     *timestamp.Timestamp
}

// NewSQL creates a new SQL graph database interface
func NewSQL(conf Config) (gdbi.GraphDB, error) {
	db, err := sql.Open(conf.Driver, conf.URL)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	ts := timestamp.NewTimestamp()
	return &SQL{db, conf.Graphs, &ts}, nil
}

// Close the connection
func (db *SQL) Close() error {
	return db.db.Close()
}

// AddGraph creates a new graph named `graph`
func (db *SQL) AddGraph(graph string) error {
	return errors.New("not implemented")
}

// DeleteGraph deletes an existing graph named `graph`
func (db *SQL) DeleteGraph(graph string) error {
	return errors.New("not implemented")
}

// GetGraphs lists the graphs managed by this driver
func (db *SQL) GetGraphs() []string {
	out := []string{}
	for k := range db.graphs {
		out = append(out, k)
	}
	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (db *SQL) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range db.GetGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &Graph{
		db:     db,
		ts:     db.ts,
		graph:  graph,
		schema: db.graphs[graph],
	}, nil
}
