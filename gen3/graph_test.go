package gen3

import (
	"testing"

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
