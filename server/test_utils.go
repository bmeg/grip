package server

import (
	"os"

	"github.com/bmeg/grip/example"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
)

func SetupTestServer(graph string) (gripql.Client, func(), error) {
	c := testConfig()
	kvPath := c.WorkDir + ".kv.db"

	cleanup := func() {
		os.RemoveAll(c.WorkDir)
		os.RemoveAll(kvPath)
	}

	kv, err := kvgraph.NewKVInterface("badger", kvPath, nil)
	if err != nil {
		return gripql.Client{}, cleanup, err
	}

	db := kvgraph.NewKVGraph(kv)
	srv, err := NewGripServer(db, c, nil)
	if err != nil {
		cleanup()
		return gripql.Client{}, cleanup, err
	}

	queryClient := gripql.NewQueryDirectClient(srv)
	editClient := gripql.NewEditDirectClient(srv)
	client := gripql.WrapClient(queryClient, editClient)

	err = client.AddGraph(graph)
	if err != nil {
		cleanup()
		return gripql.Client{}, cleanup, err
	}

	for _, v := range example.SWVertices {
		if err := client.AddVertex(graph, v); err != nil {
			cleanup()
			return gripql.Client{}, cleanup, err
		}
	}

	for _, e := range example.SWEdges {
		if err := client.AddEdge(graph, e); err != nil {
			cleanup()
			return gripql.Client{}, cleanup, err
		}
	}

	return client, cleanup, nil
}
