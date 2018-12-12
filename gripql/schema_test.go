package gripql

import (
	"testing"
)

var testSchema = `
- graph: example1
  vertices:
    - label: Human
      data:
        name: STRING
        height: NUMERIC
        mass: NUMERIC
        age: NUMERIC
        homePlanet: STRING
    - label: Droid
      data:
        name: STRING
        primaryFunction: STRING
  edges:
    - label: Owns
      from: Human
      to: Droid
      data:
        years: NUMERIC
- graph: example2
  vertices:
    - label: V1
    - label: V2
  edges:
    - label: E1
      from: V1
      to: V2
`

func TestParseSchema(t *testing.T) {
	s := []byte(testSchema)
	schemas, err := ParseSchema(s)
	t.Log(schemas)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 2 {
		t.Fatal("unexepcted schema parsing result")
	}
	vData := schemas[0].Vertices[0].GetDataMap()
	if len(vData) != 5 {
		t.Fatal("unexepcted schema parsing result")
	}
	eData := schemas[0].Edges[0].GetDataMap()
	if len(eData) != 1 {
		t.Fatal("unexepcted schema parsing result")
	}
}
