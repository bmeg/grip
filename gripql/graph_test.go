package gripql

import (
	"reflect"
	"testing"
)

var testGraph = `
- graph: example1
  vertices:
    - gid: Human
      label: Human
      data:
        name: STRING
        height: NUMERIC
        mass: NUMERIC
        age: NUMERIC
        homePlanet: STRING
    - gid: Droid
      label: Droid
      data:
        name: STRING
        primaryFunction: STRING
  edges:
    - gid: (Human)--Owns->(Droid)
      label: Owns
      from: Human
      to: Droid
      data:
        years: NUMERIC
- graph: example2
  vertices:
    - gid: V1
      label: V1
    - gid: V2
      label: V2
  edges:
    - gid: (V1)--E1->(V2)
      label: E1
      from: V1
      to: V2
`

var testGraph2 = `
graph: example2
vertices:
  - gid: V1
    label: V1
  - gid: V2
    label: V2
edges:
  - gid: (V1)--E1->(V2)
    label: E1
    from: V1
    to: V2
`

// missing graph name
var testGraph3 = `
vertices:
  - gid: V1
    label: V1
  - gid: V2
    label: V2
edges:
  - gid: (V1)--E1->(V2)
    label: E1
    from: V1
    to: V2
`

func TestParseYAMLGraph(t *testing.T) {
	s := []byte(testGraph)
	graphs, err := ParseYAMLGraphs(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(graphs) != 2 {
		t.Fatal("unexepcted graph parsing result")
	}
	vData := graphs[0].Vertices[0].GetDataMap()
	if len(vData) != 5 {
		t.Fatal("unexepcted graph parsing result")
	}
	eData := graphs[0].Edges[0].GetDataMap()
	if len(eData) != 1 {
		t.Fatal("unexepcted graph parsing result")
	}

	s = []byte(testGraph2)
	graphs, err = ParseYAMLGraphs(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(graphs) != 1 {
		t.Fatal("unexepcted graph parsing result")
	}

	s = []byte(testGraph3)
	graphs, err = ParseYAMLGraphs(s)
	if err == nil {
		t.Fatal("expected error for graph graph with no graph name")
	}
}

func TestGraphToYAML(t *testing.T) {
	s := []byte(testGraph)
	graphs, err := ParseYAMLGraphs(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := `edges:
- data:
    years: NUMERIC
  from: Human
  gid: (Human)--Owns->(Droid)
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
  gid: Human
  label: Human
- data:
    name: STRING
    primaryFunction: STRING
  gid: Droid
  label: Droid
`

	actual, err := GraphToYAMLString(graphs[0])
	if err != nil {
		t.Fatal(err)
	}

	if actual != expected {
		t.Logf("expected: \n%+v\n", expected)
		t.Logf("actual: \n%+v\n", actual)
		t.Fatal("GraphToYAML returned an unexpected result")
	}

	s2 := []byte(expected)
	graphs2, err := ParseYAMLGraphs(s2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(graphs2[0], graphs[0]) {
		t.Log(graphs2)
		t.Fatal("Failed to reparse result of GraphToYAML")
	}
}

func TestGraphToJSON(t *testing.T) {
	s := []byte(testGraph)
	graphs, err := ParseYAMLGraphs(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{
  "graph": "example1",
  "vertices": [
    {
      "gid": "Human",
      "label": "Human",
      "data": {
          "age": "NUMERIC",
          "height": "NUMERIC",
          "homePlanet": "STRING",
          "mass": "NUMERIC",
          "name": "STRING"
        }
    },
    {
      "gid": "Droid",
      "label": "Droid",
      "data": {
          "name": "STRING",
          "primaryFunction": "STRING"
        }
    }
  ],
  "edges": [
    {
      "gid": "(Human)--Owns-\u003e(Droid)",
      "label": "Owns",
      "from": "Human",
      "to": "Droid",
      "data": {
          "years": "NUMERIC"
        }
    }
  ]
}`

	actual, err := GraphToJSONString(graphs[0])
	if err != nil {
		t.Fatal(err)
	}

	if actual != expected {
		t.Logf("expected: \n%+v\n", expected)
		t.Logf("actual: \n%+v\n", actual)
		t.Fatal("GraphToJSON returned an unexpected result")
	}

	s2 := []byte(expected)
	graphs2, err := ParseJSONGraphs(s2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(graphs2[0], graphs[0]) {
		t.Log(graphs2)
		t.Fatal("Failed to reparse result of GraphToJSON")
	}
}
