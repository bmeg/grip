package server

import (
	"os"
	"strings"

	"github.com/bmeg/grip/example"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/util"
)

func SetupTestServer(graph string) (gripql.Client, func(), error) {
	rand := strings.ToLower(util.RandomString(6))
	c := Config{}
	c.HostName = "localhost"
	c.HTTPPort = util.RandomPort()
	c.RPCPort = util.RandomPort()
	c.WorkDir = "grip.work." + rand
	kvPath := "grip.db." + rand

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
		if err := client.BulkAdd(&gripql.GraphElement{Graph: graph, Vertex: v}); err != nil {
			cleanup()
			return gripql.Client{}, cleanup, err
		}
	}

	for _, e := range example.SWEdges {
		if err := client.BulkAdd(&gripql.GraphElement{Graph: graph, Edge: e}); err != nil {
			cleanup()
			return gripql.Client{}, cleanup, err
		}
	}

	return client, cleanup, nil
}
