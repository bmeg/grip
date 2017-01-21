package arachne

import (
	"github.com/bmeg/arachne/boltdb"
	"github.com/bmeg/arachne/gdbi"
	"log"
	"os"
	"strings"
	"testing"
)

func getDB() gdbi.ArachneInterface {
	testFile := "./test_graph.db"
	if _, err := os.Stat(testFile); err == nil {
		os.Remove(testFile)
	}
	log.Printf("Starting")
	return boltdb.NewBoltArachne(testFile)

}

func TestVertexInsert(t *testing.T) {
	G := getDB()
	G.Query().AddV("vertex1").Property("field1", "value1").Property("field2", "value2").Run()
	G.Query().AddV("vertex2").Run()
	G.Query().AddV("vertex3").Property("field1", "value3").Property("field2", "value4").Run()
	G.Query().AddV("vertex4").Run()
	c := G.Query().V().Count().Execute()
	o := <-c
	if o.GetIntValue() != 4 {
		t.Error("wrong number of vertices")
	}

	count := 0
	for v := range G.Query().V().Execute() {
		if !strings.HasPrefix(v.GetVertex().Gid, "vertex") {
			t.Error("mis named vertex")
		}
		count += 1
	}
	if count != 4 {
		t.Error("wrong number of vertices")
	}

	c = G.Query().V().Has("field1", "value3").Execute()
	count = 0
	for v := range c {
		if v.GetVertex().Gid != "vertex3" {
			t.Error("Filter Failed")
		}
		count++
	}
	if count != 1 {
		t.Error("wrong number of vertices")
	}
	G.Close()
}

func TestEdgeInsert(t *testing.T) {
	G := getDB()

	G.Query().AddV("vertex1").Property("field1", "value1").Run()
	G.Query().AddV("vertex2").Property("field1", "value2").Run()
	G.Query().AddV("vertex3").Property("field1", "value3").Run()
	G.Query().AddV("vertex4").Property("field1", "value4").Run()

	G.Query().V("vertex1").AddE("friend").To("vertex2").Run()
	G.Query().V("vertex2").AddE("friend").To("vertex3").Run()
	G.Query().V("vertex2").AddE("parent").To("vertex4").Run()

	for i := range G.Query().E().Execute() {
		log.Printf("%#v", i.GetEdge())
	}

	for i := range G.Query().V("vertex1").Out().Execute() {
		if i.GetVertex().Gid != "vertex2" {
			t.Error("Found wrong vertex")
		}
	}

	for i := range G.Query().V("vertex1").Out().Out().Has("field1", "value4").In().Execute() {
		if i.GetVertex().Gid != "vertex2" {
			t.Error("Found wrong vertex")
		}
	}

	G.Close()
}

func buildGraph(G gdbi.ArachneInterface) {
  G.Query().AddV("vertex1").Property("field1", "value1").Run()
	G.Query().AddV("vertex2").Property("field1", "value2").Run()
	G.Query().AddV("vertex3").Property("field1", "value3").Run()
	G.Query().AddV("vertex4").Property("field1", "value4").Run()

	G.Query().V("vertex1").AddE("friend").To("vertex2").Run()
	G.Query().V("vertex2").AddE("friend").To("vertex3").Run()
	G.Query().V("vertex2").AddE("parent").To("vertex4").Run()

}


