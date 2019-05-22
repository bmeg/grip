package gen3

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/util"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

// Config the configuration for the sql driver.
// See https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters for details.
type Config struct {
	Host      string
	Port      uint
	User      string
	Password  string
	DBName    string
	SSLMode   string
	SchemaDir string
}

// GraphDB manages graphs in the database
type GraphDB struct {
	graph  string
	db     *sqlx.DB
	layout *graphConfig
}

// NewGraphDB creates a new GraphDB graph database interface
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	log.Info("Starting Gen3 Postgres Driver")

	connString, err := util.BuildPostgresConnStr(
		conf.Host, conf.Port, conf.User, conf.Password, conf.DBName, conf.SSLMode,
	)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(10)

	layout, err := getGraphConfig(conf.SchemaDir)
	if err != nil {
		return nil, err
	}

	// TODO: ensure expected tables exist

	gdb := &GraphDB{
		graph:  conf.DBName,
		db:     db,
		layout: layout,
	}
	return gdb, nil
}

// Close the connection
func (db *GraphDB) Close() error {
	return db.db.Close()
}

// AddGraph creates a new graph named `graph`
func (db *GraphDB) AddGraph(graph string) error {
	return fmt.Errorf("not implemented")
}

// DeleteGraph deletes an existing graph named `graph`
func (db *GraphDB) DeleteGraph(graph string) error {
	return fmt.Errorf("not implemented")
}

// ListGraphs lists the graphs managed by this driver
func (db *GraphDB) ListGraphs() []string {
	return []string{db.graph}
}

// Graph obtains the gdbi.DBI for a particular graph
func (db *GraphDB) Graph(graph string) (gdbi.GraphInterface, error) {
	if graph != db.graph {
		return nil, fmt.Errorf(
			"invalid graph selection '%s'; db contains a single graph: '%s'",
			graph, db.graph,
		)
	}

	return &Graph{
		db:     db.db,
		layout: db.layout,
		psql:   sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}, nil
}
