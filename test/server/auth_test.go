package server

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmeg/grip/accounts"
	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/server"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/duration"
	"github.com/bmeg/grip/util/rpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestMain(m *testing.M) {
	var configFile string
	flag.StringVar(&configFile, "config", configFile, "config file to use for tests")
	flag.Parse()
	m.Run()
}

func TestBasicAuthFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := config.DefaultConfig()
	conf.AddBadgerDefault()
	config.TestifyConfig(conf)

	conf.Server.Accounts = accounts.Config{
		Auth: &accounts.AuthConfig{
			Basic: &accounts.BasicAuth{accounts.BasicCredential{User: "testuser", Password: "abc123"}},
		},
	}

	defer os.RemoveAll(conf.Server.WorkDir)
	srv, err := server.NewGripServer(conf, "./", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(*conf.Drivers[conf.Default].Badger)

	go srv.Serve(ctx)

	cli, err := gripql.Connect(rpc.Config{ServerAddress: conf.Server.RPCAddress(), Timeout: duration.Duration(5 * time.Second)}, true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cli.Traversal(context.Background(), &gripql.GraphQuery{Graph: "test", Query: gripql.NewQuery().V().Statements})
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Errorf("expected PermissionDenied error; got: %v", err)
	}

	_, err = cli.ListGraphs()
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Errorf("expected PermissionDenied error; got: %v", err)
	}

	_, err = cli.GetVertex("test", "1")
	if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
		t.Errorf("expected PermissionDenied error; got: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/v1/graph", conf.Server.HTTPPort))
	if err != nil {
		t.Errorf("unexpected error; got: %v", err)
	} else if resp.StatusCode != 401 {
		t.Errorf("expected http 401 error; got: %v", resp.StatusCode)
	}
}

func TestBasicAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	conf := config.DefaultConfig()
	config.TestifyConfig(conf)

	conf.Server.Accounts = accounts.Config{
		Auth: &accounts.AuthConfig{
			Basic: &accounts.BasicAuth{accounts.BasicCredential{User: "testuser", Password: "abc123"}},
		},
	}
	defer os.RemoveAll(conf.Server.WorkDir)

	os.Setenv("GRIP_USER", "testuser")
	os.Setenv("GRIP_PASSWORD", "abc123")
	defer os.Unsetenv("GRIP_USER")
	defer os.Unsetenv("GRIP_PASSWORD")

	tmpDB := "grip.db." + util.RandomString(6)
	gdb, err := kvgraph.NewKVGraphDB("badger", tmpDB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpDB)

	srv, err := server.NewGripServer(conf, "./", map[string]gdbi.GraphDB{"badger": gdb})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go srv.Serve(ctx)

	cli, err := gripql.Connect(rpc.ConfigWithDefaults(conf.Server.RPCAddress()), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cli.AddGraph("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cli.AddVertex("test", &gripql.Vertex{Gid: "1", Label: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = cli.Traversal(context.Background(), &gripql.GraphQuery{Graph: "test", Query: gripql.NewQuery().V().Statements})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = cli.ListGraphs()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = cli.GetVertex("test", "1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/v1/graph", conf.Server.HTTPPort), nil)
	req.SetBasicAuth("testuser", "abc123")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Errorf("unexpected error: %v", err)
	}
	returnString := `{"graphs":["test"]}`
	bodyText, err := ioutil.ReadAll(resp.Body)
	if string(bodyText) != returnString {
		t.Log(string(bodyText))
		t.Error("incorrect http return value")
	}
	cancel()
}

func TestCasbinAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	conf := config.DefaultConfig()
	config.TestifyConfig(conf)

	conf.Server.Accounts = accounts.Config{
		Auth: &accounts.AuthConfig{
			Basic: &accounts.BasicAuth{
				accounts.BasicCredential{User: "alice", Password: "abcd"},
				accounts.BasicCredential{User: "bob", Password: "1234"},
			},
		},
		Access: &accounts.AccessConfig{
			Casbin: &accounts.CasbinAccess{
				Model:  "../model.conf",
				Policy: "../users.csv",
			},
		},
	}
	defer os.RemoveAll(conf.Server.WorkDir)

	tmpDB := "grip.db." + util.RandomString(6)
	gdb, err := kvgraph.NewKVGraphDB("badger", tmpDB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpDB)

	srv, err := server.NewGripServer(conf, "./", map[string]gdbi.GraphDB{"badger": gdb})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go srv.Serve(ctx)

	os.Setenv("GRIP_USER", "alice")
	os.Setenv("GRIP_PASSWORD", "abcd")
	defer os.Unsetenv("GRIP_USER")
	defer os.Unsetenv("GRIP_PASSWORD")

	cli, err := gripql.Connect(rpc.ConfigWithDefaults(conf.Server.RPCAddress()), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cli.AddGraph("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cli.AddGraph("test1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cli.AddGraph("test2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/v1/graph", conf.Server.HTTPPort), nil)
	req.SetBasicAuth("alice", "abcd")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Errorf("unexpected error: %v", err)
	}

	fmt.Printf("Doing grpc traversal\n")
	q := &gripql.GraphQuery{Graph: "test", Query: gripql.NewQuery().V().Statements}
	if tcli, err := cli.QueryC.Traversal(ctx, q); err == nil {
		for {
			t, err := tcli.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("Recv Error: %s\n", err)
				break
			}
			fmt.Printf("Got traversal output: %#v\n", t.GetVertex())
		}
	} else {
		fmt.Printf("Traversal got an error\n")
	}

	fmt.Printf("Doing http traversal\n")
	resp, err = httpQuery(conf.Server.HTTPPort, "test1", "bob", "1234", q)
	if err != nil || resp.StatusCode != 200 {
		t.Errorf("unexpected error: %v, status: %d", err, resp.StatusCode)
	}

	/*
		//this would test is write access inherits attributes of read access
			fmt.Printf("Doing http traversal\n")
			resp, err = httpQuery(conf.Server.HTTPPort, "test2", "bob", "1234", q)
			if err != nil || resp.StatusCode != 200 {
				t.Errorf("unexpected error: %v", err)
			}
	*/

	fmt.Printf("Doing http traversal\n")
	resp, err = httpQuery(conf.Server.HTTPPort, "test1", "bob", "abcd", q)
	if err != nil || resp.StatusCode != 401 {
		t.Errorf("Did not receive expected auth error error: %v", err)
	}

	o, _ := io.ReadAll(resp.Body)
	fmt.Printf("post: %d %s\n", resp.StatusCode, o)

	cancel()
}

func httpQuery(port, graph, user, password string, q *gripql.GraphQuery) (*http.Response, error) {
	b, _ := protojson.Marshal(q)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/v1/graph/%s/query", port, graph), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, password)
	client := &http.Client{}
	return client.Do(req)
}
