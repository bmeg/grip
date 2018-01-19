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
		count++
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

func TestEdgeProp(t *testing.T) {
	G := getDB()

	G.Query().AddV("vertex1").Property("field1", "value1").Run()
	G.Query().AddV("vertex2").Property("field1", "value2").Run()
	G.Query().AddV("vertex3").Property("field1", "value3").Run()
	G.Query().AddV("vertex4").Property("field1", "value4").Run()

	G.Query().V("vertex1").AddE("friend").To("vertex2").Property("edgeNumber", "1").Run()
	G.Query().V("vertex2").AddE("friend").To("vertex3").Property("edgeNumber", "2").Run()
	G.Query().V("vertex2").AddE("parent").To("vertex4").Property("edgeNumber", "3").Run()

	for i := range G.Query().E().Has("edgeNumber", "1").Execute() {
		log.Printf("%#v", i.GetEdge())
	}

	i, _ := G.Query().E().Has("edgeNumber", "1").Count().First()
	if i.GetIntValue() != 1 {
		t.Error("Wrong Vertex Count")
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

func TestPropertyTypes(t *testing.T) {
	G := getDB()

	G.Query().AddV("vertex1").Property("field1", 1).Run()
	G.Query().AddV("vertex2").Property("field1", 1.1).Run()
	G.Query().AddV("vertex3").Property("field1", true).Run()
	G.Query().AddV("vertex3").Property("field1", true).Run()
	G.Query().AddV("vertex4").Property("field1", map[string]interface{}{"hello": "world"}).Run()

	v, _ := G.Query().V("vertex1").First()
	if v.GetVertex().Properties.Fields["field1"].GetNumberValue() != 1 {
		t.Errorf("Vertex wrong value")
	}

	v, _ = G.Query().V("vertex2").First()
	if v.GetVertex().Properties.Fields["field1"].GetNumberValue() != 1.1 {
		t.Errorf("Vertex wrong value")
	}

	v, _ = G.Query().V("vertex3").First()
	if v.GetVertex().Properties.Fields["field1"].GetBoolValue() != true {
		t.Errorf("Vertex wrong value")
	}

	v, _ = G.Query().V("vertex4").First()
	if v.GetVertex().Properties.Fields["field1"].GetStructValue().Fields["hello"].GetStringValue() != "world" {
		t.Errorf("Vertex wrong value")
	}

	G.Close()
}
