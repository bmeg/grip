package test

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bmeg/grip/cypher"
	"github.com/bmeg/grip/gripql"
)

func QueryCompare(a *gripql.Query, b *gripql.Query) bool {

	if len(a.Statements) != len(b.Statements) {
		return false
	}
	for i := range a.Statements {
		x := a.Statements[i]
		y := b.Statements[i]

		if !proto.Equal(x, y) {
			fmt.Printf("%#v != %#v\n", (x.Statement.(*gripql.GraphStatement_Has)).Has, (y.Statement.(*gripql.GraphStatement_Has).Has))
			return false
		}
	}
	return true
}

type testPair struct {
	cypher string
	gripql *gripql.Query
}

var pairs = []testPair{
	{
		"MATCH (n:Person {name: 'Bob'}) RETURN n",
		gripql.NewQuery().V().HasLabel("Person").Has(gripql.Eq("name", "Bob")).As("n").Render("$n"),
	},
}

func TestMatch1(t *testing.T) {

	for i := range pairs {
		p := pairs[i].gripql
		ct := pairs[i].cypher
		o, err := cypher.RunParser(ct)
		if err != nil {
			t.Error(err)
		}
		if !QueryCompare(o, p) {
			t.Errorf("Compiled query %s results in\n %s !=\n %s", ct, o.String(), p.String())
		}
	}
}
