package server

import (
	"context"
	"crypto/rand" // key for random
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/server"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func BenchmarkGripServerBulkAdd(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := config.DefaultConfig()
	conf.AddGridsDefault()
	conf.Drivers["grids"].Grids.BulkLoaderWorkers = 2
	config.TestifyConfig(conf)

	defer os.RemoveAll(conf.Server.WorkDir)

	tmpDB := "grip.db." + util.RandomString(6)
	gdb, err := grids.NewGraphDB(grids.Config{GraphDir: tmpDB})
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpDB)

	srv, err := server.NewGripServer(conf, "./", map[string]gdbi.GraphDB{"grids": gdb})
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	go srv.Serve(ctx)
	time.Sleep(1 * time.Second)

	cli, err := gripql.Connect(rpc.Config{ServerAddress: conf.Server.RPCAddress()}, true)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	graphName := "bench_graph"
	err = cli.AddGraph(graphName)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	// Add Index!
	_, err = cli.EditC.AddIndex(context.Background(), &gripql.IndexID{Graph: graphName, Label: "test_label", Field: "data"})
	if err != nil {
		b.Fatalf("add index error: %v", err)
	}

	// Prepare Data (1KB string)
	payload := make([]byte, 1024)
	rand.Read(payload)
	payloadStr := string(payload)

	b.ResetTimer()

	elemChan := make(chan *gripql.GraphElement, 100)
	errChan := make(chan error, 1)

	go func() {
		if err := cli.BulkAdd(elemChan); err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	data, _ := structpb.NewStruct(map[string]any{
		"data": payloadStr,
	})

	for i := 0; i < b.N; i++ {
		elemChan <- &gripql.GraphElement{
			Graph: graphName,
			Vertex: &gripql.Vertex{
				Id:    fmt.Sprintf("v%d", i),
				Label: "test_label",
				Data:  data,
			},
		}
	}
	close(elemChan)

	if err := <-errChan; err != nil {
		b.Fatalf("BulkAdd error: %v", err)
	}
}

func BenchmarkGripServerTraversal(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := config.DefaultConfig()
	conf.AddGridsDefault()
	conf.Drivers["grids"].Grids.BulkLoaderWorkers = 10
	config.TestifyConfig(conf)

	defer os.RemoveAll(conf.Server.WorkDir)

	tmpDB := "grip.db." + util.RandomString(6)
	gdb, err := grids.NewGraphDB(grids.Config{GraphDir: tmpDB, BulkLoaderWorkers: 10})
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpDB)

	srv, err := server.NewGripServer(conf, "./", map[string]gdbi.GraphDB{"grids": gdb})
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	go srv.Serve(ctx)
	time.Sleep(1 * time.Second)

	cli, err := gripql.Connect(rpc.Config{ServerAddress: conf.Server.RPCAddress()}, true)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	graphName := "bench_graph"
	err = cli.AddGraph(graphName)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	// Prepare data
	numRows := 10000
	payload := make([]byte, 1024)
	rand.Read(payload)
	payloadStr := string(payload)

	elemChan := make(chan *gripql.GraphElement, 100)
	go func() {
		data, _ := structpb.NewStruct(map[string]any{"data": payloadStr})
		for i := 0; i < numRows; i++ {
			elemChan <- &gripql.GraphElement{
				Graph: graphName,
				Vertex: &gripql.Vertex{
					Id:    fmt.Sprintf("v%d", i),
					Label: "test_label",
					Data:  data,
				},
			}
		}
		close(elemChan)
	}()

	if err := cli.BulkAdd(elemChan); err != nil {
		b.Fatalf("BulkAdd error: %v", err)
	}

	query := gripql.V().HasLabel("test_label")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res, err := cli.Traversal(context.Background(), &gripql.GraphQuery{Graph: graphName, Query: query.Statements})
		if err != nil {
			b.Fatalf("Traversal error: %v", err)
		}
		count := 0
		for range res {
			count++
		}
		if count != numRows {
			b.Fatalf("expected %d rows, got %d", numRows, count)
		}
	}
}
