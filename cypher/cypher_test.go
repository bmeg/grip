package cypher

import (
	"fmt"
	"testing"
)


func TestMatch1(t *testing.T) {
	//t1 := "MATCH (p:Person)-[:LIKES]->(t:Technology) RETURN p"
  //RunParser(t1)
  //fmt.Printf("Done\n")

  //t2 := "MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c) RETURN p"
  //RunParser(t2)

	t3 := "MATCH (n:Person {name: 'Bob'}) RETURN n"
	o3 := RunParser(t3)
	fmt.Printf("%s\n", t3)
	fmt.Printf("%s\n", o3.String())
}
