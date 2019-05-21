package gen3

import (
  "fmt"
	"reflect"
  "sort"
	"testing"

	//"github.com/davecgh/go-spew/spew"
)

func TestEdgeTablename(t *testing.T) {
	expected := "edge_07f60fb1_susomudefrregr"
	actual := edgeTablename("submitted_somatic_mutation", "derived_from", "read_group")
	if actual != expected {
		t.Errorf("unexpected tablename: %s != %s", actual, expected)
	}

	expected = "edge_projectmemberofprogram"
	actual = edgeTablename("project", "member_of", "program")
	if actual != expected {
		t.Errorf("unexpected tablename: %s != %s", actual, expected)
	}
}

func TestVertexTablename(t *testing.T) {
	expected := "node_program"
	actual := vertexTablename("program")
	if actual != expected {
		t.Errorf("unexpected tablename: %s != %s", actual, expected)
	}
}

func TestGetGraphConfig(t *testing.T) {
	path := "./example-json-schemas"
	actual, err := getGraphConfig(path)
	if err != nil {
		t.Error(err)
	}
	expected := &graphConfig{
		vertices: map[string]*vertexDef{
			"program": &vertexDef{
				table: "node_program",
				in: map[string][]*edgeDef{
					"member_of": []*edgeDef{
						{
							table:    "edge_projectmemberofprogram",
							srcLabel: "project",
							dstLabel: "program",
							backref:  false,
						},
					},
				},
				out: map[string][]*edgeDef{
					"projects": []*edgeDef{
						{
							table:    "edge_projectmemberofprogram",
							srcLabel: "project",
							dstLabel: "program",
							backref:  true,
						},
					},
				},
			},
			"project": &vertexDef{
				table: "node_project",
				in: map[string][]*edgeDef{
					"projects": []*edgeDef{
						{
							table:    "edge_projectmemberofprogram",
							srcLabel: "project",
							dstLabel: "program",
							backref:  true,
						},
					},
					"performed_for": []*edgeDef{
						{
							table:    "edge_experimentperformedforproject",
							srcLabel: "experiment",
							dstLabel: "project",
							backref:  false,
						},
					},
				},
				out: map[string][]*edgeDef{
					"member_of": []*edgeDef{
						{
							table:    "edge_projectmemberofprogram",
							srcLabel: "project",
							dstLabel: "program",
							backref:  false,
						},
					},
					"experiments": []*edgeDef{
						{
							table:    "edge_experimentperformedforproject",
							srcLabel: "experiment",
							dstLabel: "project",
							backref:  false,
						},
					},
				},
			},
			"experiment": &vertexDef{
				table: "node_experiement",
				in: map[string][]*edgeDef{
					"experiments": []*edgeDef{
						{
							table:    "edge_experimentperformedforproject",
							srcLabel: "experiment",
							dstLabel: "project",
							backref:  true,
						},
					},
					"member_of": []*edgeDef{
						{
							table:    "edge_casememberofexperiment",
							srcLabel: "case",
							dstLabel: "experiment",
							backref:  false,
						},
					},
				},
				out: map[string][]*edgeDef{
					"performed_for": []*edgeDef{
						{
							table:    "edge_experimentperformedforproject",
							srcLabel: "experiment",
							dstLabel: "project",
							backref:  false,
						},
					},
					"cases": []*edgeDef{
						{
							table:    "edge_casememberofexperiment",
							srcLabel: "case",
							dstLabel: "experiment",
							backref:  true,
						},
					},
				},
			},
			"case": &vertexDef{
				table: "node_case",
				in: map[string][]*edgeDef{
					"cases": []*edgeDef{
						{
							table:    "edge_casememberofexperiment",
							srcLabel: "case",
							dstLabel: "experiment",
							backref:  true,
						},
					},
				},
				out: map[string][]*edgeDef{
					"member_of": []*edgeDef{
						{
							table:    "edge_casememberofexperiment",
							srcLabel: "case",
							dstLabel: "experiment",
							backref:  false,
						},
					},
				},
			},
		},
		edges: map[string][]*edgeDef{
			"cases": []*edgeDef{
				{
					table:    "edge_casememberofexperiment",
					srcLabel: "case",
					dstLabel: "experiment",
					backref:  true,
				},
			},
			"experiments": []*edgeDef{
				{
					table:    "edge_experimentperformedforproject",
					srcLabel: "experiment",
					dstLabel: "project",
					backref:  true,
				},
			},
			"projects": []*edgeDef{
				{
					table:    "edge_projectmemberofprogram",
					srcLabel: "project",
					dstLabel: "program",
					backref:  true,
				},
			},
			"member_of": []*edgeDef{
				{
					table:    "edge_casememberofexperiment",
					srcLabel: "case",
					dstLabel: "experiment",
					backref:  false,
				},
				{
					table:    "edge_projectmemberofprogram",
					srcLabel: "project",
					dstLabel: "program",
					backref:  false,
				},
			},
			"performed_for": []*edgeDef{
				{
					table:    "edge_experimentperformedforproject",
					srcLabel: "experiment",
					dstLabel: "project",
					backref:  false,
				},
			},
		},
	}

	if len(expected.vertices) != len(actual.vertices) {
		t.Error("unexpected number of vertices in layout")
	}
  
  if !reflect.DeepEqual(getSortedKeys(expected.vertices),getSortedKeys(actual.vertices)) {
		t.Errorf("unexpected vertex keys in layout: %v != %v", getSortedKeys(actual.vertices), getSortedKeys(expected.vertices))
	}

  for k, _ := range expected.vertices {
    e := expected.vertices[k]
    a := actual.vertices[k]
    if !reflect.DeepEqual(getSortedKeys(e.in),getSortedKeys(a.in)) {
      t.Errorf("unexpected vertex keys in layout: %v != %v", getSortedKeys(a.in), getSortedKeys(e.in))
    }
    if !reflect.DeepEqual(getSortedKeys(e.out),getSortedKeys(a.out)) {
      t.Errorf("unexpected vertex keys in layout: %v != %v", getSortedKeys(a.out), getSortedKeys(e.out))
    }
  }

  if len(expected.edges) != len(actual.edges) {
		t.Error("unexpected number of edges in layout")
	}

  if !reflect.DeepEqual(getSortedKeys(expected.edges),getSortedKeys(actual.edges)) {
		t.Errorf("unexpected edge keys in layout: %v != %v", getSortedKeys(actual.edges), getSortedKeys(expected.edges))
	}

  for k, _ := range expected.edges {
    e := expected.edges[k]
    a := actual.edges[k]
    if len(e) != len(a) {
      t.Errorf("unexpected number of edge defs for label %s: %v != %v ", k, len(a), len(e))
    }
  }
}

func getSortedKeys(input interface{}) []string {
  out := []string{}
  switch v := input.(type) {
  case map[string]*vertexDef:
    for k, _ := range v {
      out = append(out, k)
    }
  case map[string][]*edgeDef:
    for k, _ := range v {
      out = append(out, k)
    }
  default:
    panic(fmt.Sprintf("unknown type %T", input))
  }
  sort.Strings(out)
  return out
}
