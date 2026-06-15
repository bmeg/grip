package psqlx

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/psql"
)

// Config defines connection and runtime behavior for a Postgres-backed
// delegated query engine.
type Config struct {
	Host     string
	Port     uint
	User     string
	Password string
	DBName   string
	SSLMode  string

	// ExtensionSchema is where SQL entrypoints are installed.
	ExtensionSchema string
	// ExtensionFunction is the SQL function for delegated traversal execution.
	ExtensionFunction string

	// DelegateTraversal will eventually switch query execution to extension SQL.
	DelegateTraversal bool
	// RequireDelegation forces delegated execution and disables compiler fallback.
	RequireDelegation bool
	// ReadOnly rejects graph mutation APIs in this backend.
	ReadOnly bool
}

type GraphDB struct {
	base gdbi.GraphDB
	conf Config
	exec DelegatedExecutor
}

func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	base, err := psql.NewGraphDB(psql.Config{
		Host:     conf.Host,
		Port:     conf.Port,
		User:     conf.User,
		Password: conf.Password,
		DBName:   conf.DBName,
		SSLMode:  conf.SSLMode,
	})
	if err != nil {
		return nil, err
	}

	if conf.ExtensionSchema == "" {
		conf.ExtensionSchema = "public"
	}
	if conf.ExtensionFunction == "" {
		conf.ExtensionFunction = "grip_exec"
	}
	if !conf.ReadOnly {
		// MVP is read-only by default to match the delegated execution scope.
		conf.ReadOnly = true
	}

	var exec DelegatedExecutor
	if conf.DelegateTraversal || conf.RequireDelegation {
		se, err := newSQLDelegatedExecutor(conf)
		if err != nil {
			if conf.RequireDelegation {
				return nil, err
			}
			log.WithFields(log.Fields{"error": err}).Warn("psqlx: delegated executor unavailable; compiler fallback remains enabled")
		} else {
			exec = se
		}
	}

	return &GraphDB{base: base, conf: conf, exec: exec}, nil
}

func (db *GraphDB) AddGraph(graph string) error {
	if db.conf.ReadOnly {
		return fmt.Errorf("psqlx: AddGraph is not supported in read-only mode")
	}
	return db.base.AddGraph(graph)
}

func (db *GraphDB) DeleteGraph(graph string) error {
	if db.conf.ReadOnly {
		return fmt.Errorf("psqlx: DeleteGraph is not supported in read-only mode")
	}
	return db.base.DeleteGraph(graph)
}

func (db *GraphDB) ListGraphs() []string {
	return db.base.ListGraphs()
}

func (db *GraphDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	g, err := db.base.Graph(graphID)
	if err != nil {
		return nil, err
	}
	return &Graph{base: g, conf: db.conf, graph: graphID, exec: db.exec}, nil
}

func (db *GraphDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
	return db.base.BuildSchema(ctx, graphID, sampleN, random)
}

func (db *GraphDB) Close() error {
	if db.exec != nil {
		err := db.exec.Close()
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Warn("psqlx: delegated executor close error")
		}
	}
	return db.base.Close()
}
