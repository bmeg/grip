package test

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bmeg/grip/cypher/compiler"
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
			fmt.Printf("stmt[%d]: %#v != %#v\n", i, x, y)
			return false
		}
	}
	return true
}

type testPair struct {
	cypher    string
	gripql    *gripql.Query
	expectErr bool
}

var pairs = []testPair{
	{
		"MATCH (n:Person {name: 'Bob'}) RETURN n",
		gripql.NewQuery().V().HasLabel("Person").Has(gripql.Eq("name", "Bob")).As("n").Render("$n"),
		false,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) RETURN friend",
		gripql.NewQuery().V().As("n").Out("FRIEND").As("friend").Render("$friend"),
		false,
	}, {
		"MATCH (n)<-[:FRIEND]-(friend) RETURN friend",
		gripql.NewQuery().V().As("n").In("FRIEND").As("friend").Render("$friend"),
		false,
	}, {
		"MATCH (n)-[:FRIEND]-(friend) RETURN friend",
		gripql.NewQuery().V().As("n").Both("FRIEND").As("friend").Render("$friend"),
		false,
	}, {
		"MATCH (n:Person {name: 'Bob'}) RETURN n.name",
		gripql.NewQuery().V().HasLabel("Person").Has(gripql.Eq("name", "Bob")).As("n").Render(map[string]any{"n.name": "$n.name"}),
		false,
	}, {
		"MATCH (n:Person {name: 'Bob'}) RETURN n.name AS personName",
		gripql.NewQuery().V().HasLabel("Person").Has(gripql.Eq("name", "Bob")).As("n").Render(map[string]any{"personName": "$n.name"}),
		false,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) RETURN n, friend.name AS friendName",
		gripql.NewQuery().V().As("n").Out("FRIEND").As("friend").Render(map[string]any{"n": "$n", "friendName": "$friend.name"}),
		false,
	}, {
		"MATCH (n:Person) WHERE n.name='Bob' RETURN n",
		gripql.NewQuery().V().HasLabel("Person").As("n").Has(gripql.Eq("name", "Bob")).Render("$n"),
		false,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) WHERE friend.age>=30 RETURN friend",
		gripql.NewQuery().V().As("n").Out("FRIEND").As("friend").Has(gripql.Gte("age", int64(30))).Render("$friend"),
		false,
	}, {
		"MATCH (n:Person) RETURN n ORDER BY n.name",
		gripql.NewQuery().V().HasLabel("Person").As("n").Sort([]*gripql.SortField{{Field: "name", Descending: false}}).Render("$n"),
		false,
	}, {
		"MATCH (n:Person) RETURN n ORDER BY n.name DESC",
		gripql.NewQuery().V().HasLabel("Person").As("n").Sort([]*gripql.SortField{{Field: "name", Descending: true}}).Render("$n"),
		false,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) RETURN friend ORDER BY friend.age DESC, friend.name ASC",
		gripql.NewQuery().V().As("n").Out("FRIEND").As("friend").Sort([]*gripql.SortField{{Field: "age", Descending: true}, {Field: "name", Descending: false}}).Render("$friend"),
		false,
	}, {
		"MATCH (n:Person) RETURN n SKIP 5",
		gripql.NewQuery().V().HasLabel("Person").As("n").Render("$n").Skip(5),
		false,
	}, {
		"MATCH (n:Person) RETURN n LIMIT 10",
		gripql.NewQuery().V().HasLabel("Person").As("n").Render("$n").Limit(10),
		false,
	}, {
		"MATCH (n:Person) RETURN n SKIP 5 LIMIT 10",
		gripql.NewQuery().V().HasLabel("Person").As("n").Render("$n").Skip(5).Limit(10),
		false,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) WHERE n.name='John' RETURN friend",
		nil,
		true,
	}, {
		"MATCH (n)-[:FRIEND]->(friend) RETURN friend ORDER BY n.name",
		nil,
		true,
	}, {
		"MATCH (n:Person) RETURN n LIMIT foo",
		nil,
		true,
	}, {
		"MATCH (n:Person) RETURN count(n)",
		nil,
		true,
	}, {
		`MATCH (n {name: 'John'})-[:FRIEND]-(friend) 
		WITH n, count(friend) AS friendsCount
		WHERE friendsCount > 3
		RETURN n, friendsCount`,
		nil,
		true,
	}, {
		`MATCH (n {name: 'John'})-[:FRIEND]-(friend)
		WITH n, count(friend) AS friendsCount
		SET n.friendsCount = friendsCount
		RETURN n.friendsCount`,
		nil,
		true,
	}, {
		`MATCH (user:User {name: 'Adam'})-[r1:FRIEND]-()-[r2:FRIEND]-(friend_of_a_friend)
		RETURN friend_of_a_friend.name AS fofName`,
		gripql.NewQuery().V().HasLabel("User").Has(gripql.Eq("name", "Adam")).As("user").Both("FRIEND").Both("FRIEND").As("friend_of_a_friend").Render(map[string]any{"fofName": "$friend_of_a_friend.name"}),
		false,
	}, {
		`MATCH (me)-[:KNOWS*1..2]-(remote_friend)
		WHERE me.name = 'Filipa'
		RETURN remote_friend.name`,
		nil,
		true,
	},
}

func TestMatch1(t *testing.T) {

	for i := range pairs {
		p := pairs[i].gripql
		ct := pairs[i].cypher
		o, err := compiler.RunParser(ct)
		if pairs[i].expectErr {
			if err == nil {
				t.Errorf("Expected compile error for query: %s", ct)
			}
			continue
		}
		if err != nil {
			t.Errorf("Unexpected compile error for query %q: %v", ct, err)
			continue
		}
		if !QueryCompare(o, p) {
			t.Errorf("Compiled query %s results in\n %s !=\n %s", ct, o.String(), p.String())
		}
	}
}
