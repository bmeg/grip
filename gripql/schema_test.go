package gripql

import (
	"reflect"
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

var testSchema2 = `
graph: example2
vertices:
  - label: V1
  - label: V2
edges:
  - label: E1
    from: V1
    to: V2
`

// missing graph name
var testSchema3 = `
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

	s = []byte(testSchema2)
	schemas, err = ParseSchema(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 1 {
		t.Fatal("unexepcted schema parsing result")
	}

	s = []byte(testSchema3)
	schemas, err = ParseSchema(s)
	if err == nil {
		t.Fatal("expected error for graph schema with no graph name")
	}
}

func TestSchemaToYAML(t *testing.T) {
	s := []byte(testSchema)
	schemas, err := ParseSchema(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := `edges:
- data:
    years: NUMERIC
  from: Human
  label: Owns
  to: Droid
graph: example1
vertices:
- data:
    age: NUMERIC
    height: NUMERIC
    homePlanet: STRING
    mass: NUMERIC
    name: STRING
  label: Human
- data:
    name: STRING
    primaryFunction: STRING
  label: Droid
`

	actual, err := SchemaToYAMLString(schemas[0])
	if err != nil {
		t.Fatal(err)
	}

	if actual != expected {
		t.Logf("expected: \n%+v\n", expected)
		t.Logf("actual: \n%+v\n", actual)
		t.Fatal("SchemaToYAML returned an unexpected result")
	}

	s2 := []byte(expected)
	schemas2, err := ParseSchema(s2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schemas2[0], schemas[0]) {
		t.Log(schemas2)
		t.Fatal("Failed to reparse result of SchemaToYAML")
	}
}
