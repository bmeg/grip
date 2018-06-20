package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmeg/arachne/aql"
	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/util"
	"github.com/bmeg/arachne/util/rpc"
)

func TestBasicAuthFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := testConfig()
	conf.BasicAuth = []BasicCredential{{User: "testuser", Password: "abc123"}}
	defer os.RemoveAll(conf.WorkDir)

	srv, err := NewArachneServer(nil, conf)
	if err != nil {
		t.Fatal(err)
	}

	go srv.Serve(ctx)

	cli, err := aql.Connect(rpc.Config{ServerAddress: conf.RPCAddress(), Timeout: 5 * time.Second}, true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cli.Traversal(&aql.GraphQuery{Graph: "test", Query: aql.NewQuery().V().Statements})
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Error("expected error")
	}

	_, err = cli.ListGraphs()
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Error("expected error")
	}

	_, err = cli.GetVertex("test", "1")
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Error("expected error")
	}
}

func TestBasicAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := testConfig()
	conf.BasicAuth = []BasicCredential{{User: "testuser", Password: "abc123"}}
	defer os.RemoveAll(conf.WorkDir)

	os.Setenv("ARACHNE_USER", "testuser")
	os.Setenv("ARACHNE_PASSWORD", "abc123")
	defer os.Unsetenv("ARACHNE_USER")
	defer os.Unsetenv("ARACHNE_PASSWORD")

	tmpDB := "arachne.db." + util.RandomString(6)
	db, err := kvgraph.NewKVGraphDB("badger", tmpDB)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDB)

	srv, err := NewArachneServer(db, conf)
	if err != nil {
		t.Fatal(err)
	}

	go srv.Serve(ctx)

	cli, err := aql.Connect(rpc.ConfigWithDefaults(conf.RPCAddress()), true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.AddGraph("test")
	if err != nil {
		t.Fatal(err)
	}

	err = cli.AddVertex("test", &aql.Vertex{Gid: "1", Label: "test"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = cli.Traversal(&aql.GraphQuery{Graph: "test", Query: aql.NewQuery().V().Statements})
	if err != nil {
		t.Error("unexpected error", err)
	}

	_, err = cli.ListGraphs()
	if err != nil {
		t.Error("unexpected error", err)
	}

	_, err = cli.GetVertex("test", "1")
	if err != nil {
		t.Error("unexpected error", err)
	}

}
