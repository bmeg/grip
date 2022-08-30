package test

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/cypher"
)

/*
func TestWrite(t *testing.T) {
	t1 := "CREATE (n {prop: 'foo'}) RETURN n.prop AS p"
	q, err := RunParser(t1)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%s\n", q.String())
}
*/

func TestMatch1(t *testing.T) {
	resetKVInterface()

	//t1 := "MATCH (p:Person)-[:LIKES]->(t:Technology) RETURN p"
	//RunParser(t1)
	//fmt.Printf("Done\n")

	//t2 := "MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c) RETURN p"
	//RunParser(t2)

	t3 := "MATCH (n:Person {name: 'Bob'}) RETURN n"
	o3, err := cypher.RunParser(t3)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Printf("%s\n", t3)
	fmt.Printf("%s\n", o3.String())
}
