package schema

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
)

type edgeDstKey struct {
	label, to string
}

type edgeDstMap map[edgeDstKey]interface{}

// ScanSchema attempts to construct a schema of a graph by sampling vertices and edges
func ScanSchema(conn gripql.Client, graph string, sampleCount uint32) (*gripql.Graph, error) {

	labelRes, err := conn.ListLabels(graph)
	if err != nil {
		return nil, err
	}

	vList := []*gripql.Vertex{}
	for _, label := range labelRes.VertexLabels {
		schema := map[string]interface{}{}
		nodeQuery := gripql.V().HasLabel(label).Limit(sampleCount)
		nodeRes, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: nodeQuery.Statements})
		if err == nil {
			for row := range nodeRes {
				v := row.GetVertex()
				data := protoutil.AsMap(v.Data)
				ds := gripql.GetDataFieldTypes(data)
				util.MergeMaps(schema, ds)
			}
			vList = append(vList, &gripql.Vertex{Gid: label, Label: "Vertex", Data: protoutil.AsStruct(schema)})
		}
	}

	eList := []*gripql.Edge{}
	for _, label := range labelRes.VertexLabels {
		edgeQuery := gripql.V().HasLabel(label).Limit(sampleCount).OutE().As("a").Out().As("b").Select("a", "b")
		edgeRes, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: edgeQuery.Statements})
		if err == nil {
			labelDstSchema := edgeDstMap{}
			for row := range edgeRes {
				sel := row.GetSelections().Selections
				edge := sel["a"].GetEdge()
				dst := sel["b"].GetVertex()
				ds := gripql.GetDataFieldTypes(protoutil.AsMap(edge.Data))
				k := edgeDstKey{to: dst.Label, label: edge.Label}
				if p, ok := labelDstSchema[k]; ok {
					labelDstSchema[k] = util.MergeMaps(p, ds)
				} else {
					labelDstSchema[k] = ds
				}
			}
			for k, v := range labelDstSchema {
				eSchema := &gripql.Edge{
					Gid:   fmt.Sprintf("(%s)-%s->(%s)", label, k.label, k.to),
					Label: k.label,
					From:  label,
					To:    k.to,
					Data:  protoutil.AsStruct(v.(map[string]interface{})),
				}
				eList = append(eList, eSchema)
			}
		}
	}
	return &gripql.Graph{Graph: graph, Vertices: vList, Edges: eList}, nil
}
