package psql

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

// Config describes the configuration for the sql driver.
// See https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters for details.
type Config struct {
	Host     string
	Port     uint
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// GraphDB manages graphs in the database
type GraphDB struct {
	db *sqlx.DB
	ts *timestamp.Timestamp
}

// NewGraphDB creates a new GraphDB graph database interface
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	log.Info("Starting Postgres Driver")

	connString := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s",
		conf.Host, conf.Port, conf.User, conf.Password, conf.DBName, conf.SSLMode,
	)
	db, err := sqlx.Connect("postgres", connString)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(5)
	ts := timestamp.NewTimestamp()
	gdb := &GraphDB{db, &ts}
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
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_vertices (gid varchar not null, label varchar not null, data jsonb)", graph)
	_, err := db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating vertex table: %v", err)
	}
	stmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_edges (gid varchar not null, label varchar not null, "from" varchar not null, "to" varchar not null, data jsonb)`, graph)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating edge table: %v", err)
	}

	vertexTable := fmt.Sprintf("%s_vertices", graph)
	toIndex := []string{"gid", "label"}
	for _, f := range toIndex {
		err := db.createIndex(vertexTable, f)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("AddGraph: creating index")
		}
	}

	edgeTable := fmt.Sprintf("%s_edges", graph)
	toIndex = []string{"gid", "label", "from", "to"}
	for _, f := range toIndex {
		err := db.createIndex(edgeTable, f)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("AddGraph: creating index")
		}
	}
	return nil
}

func (db *GraphDB) createIndex(table, field string) error {
	stmt := fmt.Sprintf("CREATE INDEX %s_%s ON %s (%s)", table, field, table, field)
	_, err := db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating index for table %s on field %s: %v", table, field, err)
	}
	return nil
}

// DeleteGraph deletes an existing graph named `graph`
func (db *GraphDB) DeleteGraph(graph string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s_vertices", graph)
	_, err := db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("dropping vertex table: %v", err)
	}
	stmt = fmt.Sprintf("DROP TABLE IF EXISTS %s_edges", graph)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("dropping edge table: %v", err)
	}
	return nil
}

// ListGraphs lists the graphs managed by this driver
func (db *GraphDB) ListGraphs() []string {
	out := []string{}
	rows, err := db.db.Queryx("SHOW TABLES")
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("ListGraphs: Queryx")
		return out
	}
	defer rows.Close()
	var table string
	for rows.Next() {
		if err := rows.Scan(&table); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("ListGraphs: Scan")
			return out
		}
		out = append(out, strings.SplitN(table, "_", 2)[0])
	}
	if err := rows.Err(); err != nil {
		log.WithFields(log.Fields{"error": err}).Error("ListGraphs: iterating")
		return out
	}
	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (db *GraphDB) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range db.ListGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &Graph{
		db:    db.db,
		v:     fmt.Sprintf("%s_vertices", graph),
		e:     fmt.Sprintf("%s_edges", graph),
		ts:    db.ts,
		graph: graph,
	}, nil
}

// GetSchema returns the schema of a specific graph in the database
func (db *GraphDB) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
	return nil, fmt.Errorf("not implemented")
}
