package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper/node"
	"github.com/bmeg/grip/gripper/node/table"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/server"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	name       string
	prefix     string
	listen     = "localhost:50051"
	driver     = "pebble"
	dbPath     = "grip-node.db"
	graphName  = "node"
	configFile string
	vertices   string
	edges      string
	mappingArg string
)

const loadBatch = 1000

func startDriver() (gdbi.GraphDB, error) {
	if configFile != "" {
		conf := config.DefaultConfig()
		if err := config.ParseConfigFile(configFile, conf); err != nil {
			return nil, err
		}
		conf.SetDefaults()
		d, ok := conf.Drivers[conf.Default]
		if !ok {
			return nil, fmt.Errorf("config has no driver named %q", conf.Default)
		}
		return server.StartDriver(d, nil)
	}
	switch driver {
	case "pebble":
		return server.StartDriver(config.DriverConfig{Pebble: &dbPath}, nil)
	case "badger":
		return server.StartDriver(config.DriverConfig{Badger: &dbPath}, nil)
	}
	return nil, fmt.Errorf("unsupported driver %q; use --config for other databases", driver)
}

func readJSONLines(path string, fn func(map[string]interface{}) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		m := map[string]interface{}{}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return err
		}
		if err := fn(m); err != nil {
			return err
		}
	}
	return sc.Err()
}

func str(m map[string]interface{}, key string) string {
	s, _ := m[key].(string)
	return s
}

func loadVertices(g gdbi.GraphInterface, path string) error {
	batch := []*gdbi.Vertex{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := g.AddVertex(batch)
		batch = batch[:0]
		return err
	}
	err := readJSONLines(path, func(m map[string]interface{}) error {
		v := &gdbi.Vertex{ID: str(m, "_id"), Label: str(m, "_label"), Loaded: true}
		if !strings.HasPrefix(v.ID, prefix) {
			return fmt.Errorf("vertex %q does not start with prefix %q", v.ID, prefix)
		}
		delete(m, "_id")
		delete(m, "_label")
		v.Data = m
		batch = append(batch, v)
		if len(batch) >= loadBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func loadEdges(g gdbi.GraphInterface, path string) error {
	batch := []*gdbi.Edge{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := g.AddEdge(batch)
		batch = batch[:0]
		return err
	}
	err := readJSONLines(path, func(m map[string]interface{}) error {
		e := &gdbi.Edge{ID: str(m, "_id"), Label: str(m, "_label"), From: str(m, "_from"), To: str(m, "_to"), Loaded: true}
		if !strings.HasPrefix(e.ID, prefix) {
			return fmt.Errorf("edge %q does not start with prefix %q", e.ID, prefix)
		}
		for _, k := range []string{"_id", "_label", "_from", "_to"} {
			delete(m, k)
		}
		e.Data = m
		batch = append(batch, e)
		if len(batch) >= loadBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// Cmd is the node command
var Cmd = &cobra.Command{
	Use:   "node",
	Short: "Run a GRIPPER resource node that serves one subgraph of a federation",
	Long: `Serves a graph over the GRIPSource gRPC API. Every vertex and edge ID in the
graph must start with --prefix. Optionally loads JSON lines files of vertices and
edges (with _id, _label, _from, _to), or tables described by --mapping, into the graph
before serving. Other databases (SQL, MongoDB, ...) can be served with --config.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if prefix == "" {
			return fmt.Errorf("--prefix is required")
		}
		if name == "" {
			name = prefix
		}
		db, err := startDriver()
		if err != nil {
			return err
		}
		defer db.Close()
		if !contains(db.ListGraphs(), graphName) {
			if err := db.AddGraph(graphName); err != nil {
				return err
			}
		}
		g, err := db.Graph(graphName)
		if err != nil {
			return err
		}
		if vertices != "" {
			if err := loadVertices(g, vertices); err != nil {
				return fmt.Errorf("loading vertices: %w", err)
			}
		}
		if edges != "" {
			if err := loadEdges(g, edges); err != nil {
				return fmt.Errorf("loading edges: %w", err)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if mappingArg != "" {
			m, err := table.LoadMapping(mappingArg)
			if err != nil {
				return err
			}
			nv, ne, err := table.Load(ctx, g, m, filepath.Dir(mappingArg), prefix)
			if err != nil {
				return fmt.Errorf("loading tables: %w", err)
			}
			log.Infof("Loaded %d vertices and %d edges from tables", nv, ne)
		}
		links, err := node.ScanEdgeLinks(ctx, g, prefix)
		if err != nil {
			return err
		}
		srv, err := node.NewServer(g, node.Options{Name: name, Prefix: prefix, EdgeLinks: links})
		if err != nil {
			return err
		}
		lis, err := net.Listen("tcp", listen)
		if err != nil {
			return err
		}
		gs := grpc.NewServer()
		srv.Register(gs)

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sig)
		go func() {
			<-sig
			log.Infof("Shutting down node %s", name)
			gs.GracefulStop()
		}()
		log.Infof("GRIPPER node %s serving prefix %s on %s (%d cross-prefix edge links)", name, prefix, listen, len(links))
		return gs.Serve(lis)
	},
}

func contains(list []string, s string) bool {
	for _, i := range list {
		if i == s {
			return true
		}
	}
	return false
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&name, "name", name, "Source name (defaults to the prefix)")
	flags.StringVar(&prefix, "prefix", prefix, "CURIE prefix of every vertex and edge ID, e.g. Character:")
	flags.StringVar(&listen, "listen", listen, "Address to listen on")
	flags.StringVar(&driver, "driver", driver, "Storage driver when no config file is given [pebble, badger]")
	flags.StringVar(&dbPath, "db", dbPath, "Database path used with --driver")
	flags.StringVarP(&configFile, "config", "c", configFile, "GRIP config file; its default driver stores the graph")
	flags.StringVar(&graphName, "graph", graphName, "Name of the graph in the database")
	flags.StringVar(&vertices, "vertices", vertices, "JSON lines file of vertices to load")
	flags.StringVar(&edges, "edges", edges, "JSON lines file of edges to load")
	flags.StringVar(&mappingArg, "mapping", mappingArg, "YAML file that maps CSV/JSON lines tables onto vertices and edges to load")
}
