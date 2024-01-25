package schema

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
	"google.golang.org/protobuf/types/known/structpb"
)

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
func ScanSchema(conn gripql.Client, graph string, sampleCount uint32, exclude []string) (*gripql.Graph, error) {

	labelRes, err := conn.ListLabels(graph)
	if err != nil {
		return nil, err
	}

	vList := []*gripql.Vertex{}
	eList := []*gripql.Edge{}
	for _, label := range labelRes.VertexLabels {
		if stringInSlice(label, exclude) {
			continue
		}
		schema := map[string]interface{}{}
		log.Infof("Scanning %s\n", label)

		nodeQuery := gripql.V().HasLabel(label).Limit(sampleCount)
		nodeRes, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: nodeQuery.Statements})
		if err == nil {
			for row := range nodeRes {
				v := row.GetVertex()
				data := v.Data.AsMap()
				ds := gripql.GetDataFieldTypes(data)
				util.MergeMaps(schema, ds)
			}
			sValue, err := structpb.NewStruct(schema)
			if err != nil {
				log.Error(err)
			}
			vList = append(vList, &gripql.Vertex{Gid: label, Label: "Vertex", Data: sValue})
		} else {
			log.Errorf("Traversal error: %s", err)
		}

		edgeQuery := gripql.V().HasLabel(label).Limit(sampleCount).Fields().As("from").OutE().As("edge").Select("edge").Out().Fields().As("to").Select("edge", "from", "to")
		edgeRes, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: edgeQuery.Statements})
		if err == nil {
			labelSchema := edgeMap{}
			for row := range edgeRes {
				sel := row.GetSelections().Selections
				edge := sel["edge"].GetEdge()
				src := sel["from"].GetVertex()
				dst := sel["to"].GetVertex()
				ds := gripql.GetDataFieldTypes(edge.Data.AsMap())
				k := edgeKey{to: dst.Label, from: src.Label, label: edge.Label}
				if p, ok := labelSchema[k]; ok {
					labelSchema[k] = util.MergeMaps(p, ds)
				} else {
					labelSchema[k] = ds
				}
			}
			for k, v := range labelSchema {
				sValue, _ := structpb.NewStruct(v.(map[string]interface{}))
				eSchema := &gripql.Edge{
					Gid:   fmt.Sprintf("(%s)-%s->(%s)", k.from, k.label, k.to),
					Label: k.label,
					From:  k.from,
					To:    k.to,
					Data:  sValue,
				}
				eList = append(eList, eSchema)
			}
		} else {
			log.Errorf("Traversal error: %s", err)
		}
	}
	return &gripql.Graph{Graph: graph, Vertices: vList, Edges: eList}, nil
}
