package psql

import (
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

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS graphs (graph_name varchar PRIMARY KEY, sanitized_graph_name varchar NOT NULL, vertex_table varchar NOT NULL, edge_table varchar NOT NULL)")
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("creating graphs table: %v", err)
	}

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
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	sanitizedName := strings.Replace(graph, "-", "_", -1)
	vertexTable := fmt.Sprintf("%s_vertices", sanitizedName)
	edgeTable := fmt.Sprintf("%s_edges", sanitizedName)

	stmt := fmt.Sprintf("INSERT INTO graphs (graph_name, sanitized_graph_name, vertex_table, edge_table) VALUES ('%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING", graph, sanitizedName, vertexTable, edgeTable)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("inserting row into graphs table: %v", err)
	}

	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (gid varchar PRIMARY KEY, label varchar NOT NULL, data jsonb)", vertexTable)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating vertex table: %v", err)
	}

	toIndex := []string{"label"}
	for _, f := range toIndex {
		err := db.createIndex(vertexTable, f)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("AddGraph: creating index")
		}
	}

	stmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (gid varchar PRIMARY KEY, label varchar NOT NULL, "from" varchar NOT NULL, "to" varchar NOT NULL, data jsonb)`, edgeTable)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating edge table: %v", err)
	}

	toIndex = []string{"label", "from", "to"}
	for _, f := range toIndex {
		err := db.createIndex(edgeTable, f)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("AddGraph: creating index")
		}
	}
	return nil
}

func (db *GraphDB) createIndex(table, field string) error {
	stmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_%s ON %s ("%s")`, table, field, table, field)
	_, err := db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("creating index for table %s on field %s: %v", table, field, err)
	}
	return nil
}

type graphInfo struct {
	GraphName          string
	SanitizedGraphName string
	VertexTable        string
	EdgeTable          string
}

func (db *GraphDB) getGraphInfo(graph string) (*graphInfo, error) {
	q := fmt.Sprintf("SELECT * FROM graphs where graph_name='%s'", graph)
	info := make(map[string]interface{})
	err := db.db.QueryRowx(q).MapScan(info)
	if err != nil {
		return nil, fmt.Errorf("querying graphs table: %v", err)
	}
	return &graphInfo{
		GraphName:          info["graph_name"].(string),
		SanitizedGraphName: info["sanitized_graph_name"].(string),
		VertexTable:        info["vertex_table"].(string),
		EdgeTable:          info["edge_table"].(string),
	}, nil
}

// DeleteGraph deletes an existing graph named `graph`
func (db *GraphDB) DeleteGraph(graph string) error {
	info, err := db.getGraphInfo(graph)
	if err != nil {
		return fmt.Errorf("DeleteGraph: %v", err)
	}

	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s_vertices", info.VertexTable)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("DeleteGraph: dropping vertex table: %v", err)
	}

	stmt = fmt.Sprintf("DROP TABLE IF EXISTS %s_edges", info.EdgeTable)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("DeleteGraph: dropping edge table: %v", err)
	}

	stmt = fmt.Sprintf("DELETE FROM graphs where graph_name='%s'", graph)
	_, err = db.db.Exec(stmt)
	if err != nil {
		return fmt.Errorf("DeleteGraph: deleting row from graphs table: %v", err)
	}

	return nil
}

// ListGraphs lists the graphs managed by this driver
func (db *GraphDB) ListGraphs() []string {
	out := []string{}
	rows, err := db.db.Queryx("SELECT graph_name FROM graphs")
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
	info, err := db.getGraphInfo(graph)
	if err != nil {
		return nil, fmt.Errorf("graph '%s' was not found: %v", graph, err)
	}
	return &Graph{
		db:    db.db,
		v:     info.VertexTable,
		e:     info.EdgeTable,
		ts:    db.ts,
		graph: graph,
	}, nil
}
