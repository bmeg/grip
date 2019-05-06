package server

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bmeg/grip/example"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/util"
)

func SetupTestingServer(ctx context.Context, graph string) (gripql.Client, error) {
	rand := strings.ToLower(util.RandomString(6))
	c := Config{}
	c.HostName = "localhost"
	c.HTTPPort = util.RandomPort()
	c.RPCPort = util.RandomPort()
	c.WorkDir = "grip.work." + rand
	kvPath := "grip.db." + rand

	kv, err := kvgraph.NewKVInterface("badger", kvPath, nil)
	if err != nil {
		return gripql.Client{}, err
	}

	db := kvgraph.NewKVGraph(kv)
	srv, err := NewGripServer(db, c, nil)
	if err != nil {
		return gripql.Client{}, err
	}

	queryClient := gripql.NewQueryDirectClient(srv)
	editClient := gripql.NewEditDirectClient(srv)
	client := gripql.WrapClient(queryClient, editClient)

	err = client.AddGraph(graph)
	if err != nil {
		return gripql.Client{}, err
	}

	elemChan := make(chan *gripql.GraphElement)
	wait := make(chan bool)
	go func() {
		if err := client.BulkAdd(elemChan); err != nil {
			fmt.Printf("BulkAdd error: %v", err)
		}
		wait <- false
	}()

	for _, v := range example.SWVertices {
		elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
	}

	for _, e := range example.SWEdges {
		elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
	}

	close(elemChan)
	<-wait

	go func() {
		select {
		case <-ctx.Done():
			kv.Close()
			os.RemoveAll(c.WorkDir)
			os.RemoveAll(kvPath)
		}
	}()

	return client, nil
}
