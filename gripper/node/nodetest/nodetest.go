// Package nodetest has helpers for testing federation code against in-process resource nodes.
package nodetest

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripper/node"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/pebbledb" // registers the pebble driver
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// NewGraph creates an empty pebble backed graph in a temporary directory.
func NewGraph(t testing.TB, name string) gdbi.GraphInterface {
	t.Helper()
	db, err := kvgraph.NewKVGraphDB("pebble", filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.AddGraph("g"); err != nil {
		t.Fatal(err)
	}
	g, err := db.Graph("g")
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// Serve starts srv on an in-memory listener and returns a client connected to it.
func Serve(t testing.TB, srv *node.Server) gripper.GRIPSourceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	srv.Register(g)
	go g.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		g.Stop()
	})
	return gripper.NewGRIPSourceClient(conn)
}
