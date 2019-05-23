package gen3

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
)

var gdb gdbi.GraphDB
var g gdbi.GraphInterface

func TestMain(m *testing.M) {
	var err error
	var exit = 1

	defer func() {
		fmt.Println("tests exiting with code", exit)
		os.Exit(exit)
	}()

	c := Config{
		Host:      "localhost",
		Port:      5432,
		User:      "postgres",
		DBName:    "metadata_db",
		SchemaDir: "/Users/strucka/Projects/gen3/compose-services/example-schemas",
	}

	gdb, err = NewGraphDB(c)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer gdb.Close()

	g, err = gdb.Graph(c.DBName)
	if err != nil {
		fmt.Println(err)
		return
	}

	// run tests
	exit = m.Run()
}

func TestGetVertex(t *testing.T) {
	v := g.GetVertex("c4fb3551-dc61-4a7a-9db0-ac2ef6700b89", true)
	t.Logf("%+v", v)
	if v == nil {
		t.Error("expected vertex, got nil")
	}
}

func TestGetVertexList(t *testing.T) {
	var outChan <-chan *gripql.Vertex
	var count, expected int

	outChan = g.GetVertexList(context.Background(), true)
	count = 0
	for range outChan {
		count++
	}
	expected = 7
	if count != expected {
		t.Errorf("with label: unexpected number of results: %v != %v", count, expected)
	}
}

func TestVertexLabelScan(t *testing.T) {
	outChan := g.VertexLabelScan(context.Background(), "case")
	count := 0
	for range outChan {
		count++
	}
	if count != 4 {
		t.Error("unexpected number of results")
	}
}

func TestGetVertexChannel(t *testing.T) {
	reqChan := make(chan gdbi.ElementLookup, 10)
	ids := []string{
		"7eef5dc2-2679-4da2-99b3-34ac991089da",
		"315358b5-b527-48c9-8d75-231d7a209cd4",
		"c182ee44-28df-4c1e-aa92-3ea9f7400945",
	}
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id}
	}
	close(reqChan)

	outChan := g.GetVertexChannel(reqChan, true)
	count := 0
	for range outChan {
		count++
	}
	if count != 3 {
		t.Error("unexpected number of results")
	}
}

func TestGetOutChannel(t *testing.T) {
	ids := []string{
		"c4fb3551-dc61-4a7a-9db0-ac2ef6700b89",
	}

	reqChan := make(chan gdbi.ElementLookup, 10)
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id, Ref: &gripql.Vertex{Gid: id, Label: "experiment"}}
	}
	close(reqChan)

	var outChan chan gdbi.ElementLookup
	var count int

	outChan = g.GetOutChannel(reqChan, true, []string{"cases"})
	count = 0
	for range outChan {
		count++
	}
	if count != 4 {
		t.Errorf("with label: unexpected number of results: %v != %v", count, 4)
	}
}

func TestGetInChannel(t *testing.T) {
	ids := []string{
		"c4fb3551-dc61-4a7a-9db0-ac2ef6700b89",
	}

	reqChan := make(chan gdbi.ElementLookup, 10)
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id, Ref: &gripql.Vertex{Gid: id, Label: "experiment"}}
	}
	close(reqChan)

	var outChan chan gdbi.ElementLookup
	var count, expected int

	outChan = g.GetInChannel(reqChan, true, []string{"member_of"})
	count = 0
	for range outChan {
		count++
	}
	expected = 4
	if count != expected {
		t.Errorf("with label: unexpected number of results: %v != %v", count, expected)
	}
}

func TestGetEdge(t *testing.T) {
	e := g.GetEdge("7eef5dc2-2679-4da2-99b3-34ac991089da_c4fb3551-dc61-4a7a-9db0-ac2ef6700b89", true)
	t.Logf("%+v", e)
	if e == nil {
		t.Error("expected edge, got nil")
	}
}

func TestGetEdgeList(t *testing.T) {
	var outChan <-chan *gripql.Edge
	var count, expected int

	outChan = g.GetEdgeList(context.Background(), true)
	count = 0
	for range outChan {
		count++
	}
	expected = 6
	if count != expected {
		t.Errorf("with label: unexpected number of results: %v != %v", count, expected)
	}
}

func TestGetOutEdgeChannel(t *testing.T) {
	ids := []string{
		"c4fb3551-dc61-4a7a-9db0-ac2ef6700b89",
	}

	reqChan := make(chan gdbi.ElementLookup, 10)
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id, Ref: &gripql.Vertex{Gid: id, Label: "experiment"}}
	}
	close(reqChan)

	var outChan chan gdbi.ElementLookup
	var count, expected int

	outChan = g.GetOutEdgeChannel(reqChan, true, []string{"cases"})
	count = 0
	for range outChan {
		count++
	}
	expected = 6
	if count != expected {
		t.Errorf("with label: unexpected number of results: %v != %v", count, expected)
	}
}

func TestGetInEdgeChannel(t *testing.T) {
	ids := []string{
		"c4fb3551-dc61-4a7a-9db0-ac2ef6700b89",
	}

	reqChan := make(chan gdbi.ElementLookup, 10)
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id, Ref: &gripql.Vertex{Gid: id, Label: "experiment"}}
	}
	close(reqChan)

	var outChan chan gdbi.ElementLookup
	var count, expected int

	outChan = g.GetOutEdgeChannel(reqChan, true, []string{"experiments"})
	count = 1
	for range outChan {
		count++
	}
	expected = 1
	if count != expected {
		t.Errorf("with label: unexpected number of results: %v != %v", count, expected)
	}
}
