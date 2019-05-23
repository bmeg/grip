package gen3

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/gdbi"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
)

func TestGetVertex(t *testing.T) {
	c := Config{
		Host:      "localhost",
		Port:      5432,
		User:      "postgres",
		DBName:    "metadata_db",
		SchemaDir: "/Users/strucka/Projects/gen3/compose-services/example-schemas",
	}
	gdb, err := NewGraphDB(c)
	if err != nil {
		t.Fatal(err)
	}
	defer gdb.Close()

	g, err := gdb.Graph(c.DBName)
	if err != nil {
		t.Fatal(err)
	}
	v := g.GetVertex("c4fb3551-dc61-4a7a-9db0-ac2ef6700b89", true)
	t.Logf("%+v", v)
	if v == nil {
		t.Error("expected vertex, got nil")
	}
}

func TestGetVertexChannel(t *testing.T) {
	c := Config{
		Host:      "localhost",
		Port:      5432,
		User:      "postgres",
		DBName:    "metadata_db",
		SchemaDir: "/Users/strucka/Projects/gen3/compose-services/example-schemas",
	}
	gdb, err := NewGraphDB(c)
	if err != nil {
		t.Fatal(err)
	}
	defer gdb.Close()
	g, err := gdb.Graph(c.DBName)
	if err != nil {
		t.Fatal(err)
	}
	reqChan := make(chan gdbi.ElementLookup, 10)
	ids := []string{
		"7eef5dc2-2679-4da2-99b3-34ac991089da",
		"315358b5-b527-48c9-8d75-231d7a209cd4",
		"c182ee44-28df-4c1e-aa92-3ea9f7400945",
		"7aea5e0b-0ff2-416b-92a4-02b94c33a020",
	}
	for _, id := range ids {
		reqChan <- gdbi.ElementLookup{ID: id}
	}
	fmt.Println("3")
	close(reqChan)
	fmt.Println("4")
	outChan := g.GetVertexChannel(reqChan, true)
	count := 0
	for e := range outChan {
		t.Logf("%+v", e)
		count++
	}
	if count != 4 {
		t.Error("unexpected number of results")
	}
}
