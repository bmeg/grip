package server

import (
	"os"

	"github.com/bmeg/grip/example"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
)

type TestServer struct {
	Config      Config
	KVStorePath string
	DB          gdbi.GraphDB
	Server      *GripServer
	Client      gripql.Client
	Graph       string
	Schema      *gripql.Graph
}

func (s *TestServer) Cleanup() {
	os.RemoveAll(s.Config.WorkDir)
	os.RemoveAll(s.KVStorePath)
}

func SetupTestServer(graph string) (*TestServer, error) {
	c := testConfig()
	kvPath := c.WorkDir + ".kv.db"

	s := &TestServer{
		Config:      c,
		KVStorePath: c.WorkDir + ".kv.db",
		Graph:       graph,
		Schema:      example.SWSchema,
	}

	kv, err := kvgraph.NewKVInterface("badger", kvPath, nil)
	if err != nil {
		s.Cleanup()
		return nil, err
	}

	db := kvgraph.NewKVGraph(kv)
	srv, err := NewGripServer(db, c, nil)
	if err != nil {
		s.Cleanup()
		return nil, err
	}

	queryClient := gripql.NewQueryDirectClient(srv)
	editClient := gripql.NewEditDirectClient(srv)
	client := gripql.WrapClient(queryClient, editClient)

	s.DB = db
	s.Server = srv
	s.Client = client

	err = client.AddGraph(graph)
	if err != nil {
		s.Cleanup()
		return nil, err
	}

	for _, v := range example.SWVertices {
		if err := client.AddVertex(graph, v); err != nil {
			s.Cleanup()
			return nil, err
		}
	}

	for _, e := range example.SWEdges {
		if err := client.AddEdge(graph, e); err != nil {
			s.Cleanup()
			return nil, err
		}
	}

	return s, nil
}
