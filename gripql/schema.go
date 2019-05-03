package gripql

import (
	"fmt"
  "strings"

	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
)

var SchemaSuffix = "__schema__"

// IsSchema reports whether a graph name refers to a schema
func IsSchema(graphName string) bool {
	return strings.HasSuffix(graphName, SchemaSuffix)
}

type edgeKey struct {
	label, to, from string
}

type edgeMap map[edgeKey]interface{}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// ScanSchema attempts to construct a schema of a graph by sampling vertices and edges
// This version of the schema scanner (vs the ones found in the drivers) can be run
// via the client library
func ScanSchema(conn Client, graph string, sampleCount uint32, exclude []string) (*Graph, error) {

	labelRes, err := conn.ListLabels(graph)
	if err != nil {
		return nil, err
	}

	vList := []*Vertex{}
	for _, label := range labelRes.VertexLabels {
		if stringInSlice(label, exclude) {
			continue
		}
		schema := map[string]interface{}{}
		nodeQuery := V().HasLabel(label).Limit(sampleCount)
		nodeRes, err := conn.Traversal(&GraphQuery{Graph: graph, Query: nodeQuery.Statements})
		if err == nil {
			for row := range nodeRes {
				v := row.GetVertex()
				data := protoutil.AsMap(v.Data)
				ds := GetDataFieldTypes(data)
				util.MergeMaps(schema, ds)
			}
			vList = append(vList, &Vertex{Gid: label, Label: label, Data: protoutil.AsStruct(schema)})
		}
	}

	eList := []*Edge{}
	for _, elabel := range labelRes.EdgeLabels {
		if stringInSlice(elabel, exclude) {
			continue
		}
		edgeQuery := E().HasLabel(elabel).Limit(sampleCount).As("edge").Out().Fields().As("to").Select("edge").In().Fields().As("from").Select("edge", "from", "to")
		edgeRes, err := conn.Traversal(&GraphQuery{Graph: graph, Query: edgeQuery.Statements})
		if err == nil {
			labelSchema := edgeMap{}
			for row := range edgeRes {
				sel := row.GetSelections().Selections
				edge := sel["edge"].GetEdge()
				src := sel["from"].GetVertex()
				dst := sel["to"].GetVertex()
				ds := GetDataFieldTypes(protoutil.AsMap(edge.Data))
				k := edgeKey{to: dst.Label, from: src.Label, label: edge.Label}
				if p, ok := labelSchema[k]; ok {
					labelSchema[k] = util.MergeMaps(p, ds)
				} else {
					labelSchema[k] = ds
				}
			}
			for k, v := range labelSchema {
				eSchema := &Edge{
					Gid:   fmt.Sprintf("(%s)--%s->(%s)", k.from, k.label, k.to),
					Label: k.label,
					From:  k.from,
					To:    k.to,
					Data:  protoutil.AsStruct(v.(map[string]interface{})),
				}
				eList = append(eList, eSchema)
			}
		}
	}
	return &Graph{Graph: graph, Vertices: vList, Edges: eList}, nil
}
