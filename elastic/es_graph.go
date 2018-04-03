package elastic

import (
	"context"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticGraph struct {
	url       string
	database  string
	ts        *timestamp.Timestamp
	client    *elastic.Client
	graph     string
	graphName string
}

// Compiler
func (es *ElasticGraph) Compiler() gdbi.Compiler {
	return core.NewCompiler(es)
}

// GetTimestamp
func (es *ElasticGraph) GetTimestamp() string {
	return es.ts.Get(es.graph)
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddEdge(edgeArray []*aql.Edge) error {
	bulkRequest := es.client.Bulk()
	for _, e := range edgeArray {
		edoc := PackEdge(e)
		req := elastic.NewBulkIndexRequest().Index(es.graphName).Type("edge").Id(e.Gid).Doc(edoc)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(context.Background())
	return err
}

// AddVertex adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddVertex(vertexArray []*aql.Vertex) error {
	bulkRequest := es.client.Bulk()
	for _, e := range vertexArray {
		edoc := PackVertex(e)
		req := elastic.NewBulkIndexRequest().Index(es.graphName).Type("vertex").Id(e.Gid).Doc(edoc)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(context.Background())
	return err
}

// AddBundle
func (es *ElasticGraph) AddBundle(bundle *aql.Bundle) error {
	return nil
}

// AddVertexIndex
func (es *ElasticGraph) AddVertexIndex(label string, field string) error {
	// body := `{
	//   "mappings": {
	//     "task":{
	//       "properties":{
	//         "id": {
	//           "type": "keyword"
	//         },
	//       }
	//     }
	//   }
	// }`
	// _, err := es.client.CreateIndex(es.graphName).Body(body).Do(context.Background())
	// return err
	return nil
}

// DelEdge
func (es *ElasticGraph) DelEdge(eid string) error {
	_, err := es.client.Delete().Index(es.graphName).Type("edge").Id(eid).Do(context.Background())
	return err
}

// DelVertex
func (es *ElasticGraph) DelVertex(vid string) error {
	//TODO: remove connected edges
	_, err := es.client.Delete().Index(es.graphName).Type("vertex").Id(vid).Do(context.Background())
	return err
}

// DelBundle
func (es *ElasticGraph) DelBundle(eid string) error {
	return nil
}

// DeleteVertexIndex
func (es *ElasticGraph) DeleteVertexIndex(label string, field string) error {
	return nil
}

// GetEdge
func (es *ElasticGraph) GetEdge(id string, loadProp bool) *aql.Edge {
	get1, err := es.client.Get().Index(es.graphName).Type("edge").Id(id).Do(context.Background())
	if err != nil {
		log.Printf("failed to get edge: %s", err)
	}
	return UnpackEdge(get1.Fields)
}

// GetVertex
func (es *ElasticGraph) GetVertex(id string, loadProp bool) *aql.Vertex {
	get1, err := es.client.Get().Index(es.graphName).Type("vertex").Id(id).Do(context.Background())
	if err != nil {
		log.Printf("failed to get vertex: %s", err)
	}
	return UnpackVertex(get1.Fields)
}

// GetBundle
func (es *ElasticGraph) GetBundle(id string, loadProp bool) *aql.Bundle {
	return nil
}

// GetEdgeList produces a channel of all edges in the graph
func (es *ElasticGraph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *aql.Edge {
	/*
		  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
			searchResult, err := client.Search().
		  		Index(graphName).
		      Type("edge").
		  		Do(context.Background())
		  for _, item := range searchResult.Each() {
		    doc := item.(map[string]interface{})
		  }
	*/
	return nil
}

// GetVertexList produces a channel of all vertices in the graph
func (es *ElasticGraph) GetVertexList(ctx context.Context, loadProp bool) <-chan *aql.Vertex {
	/*
		  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
			searchResult, err := client.Search().
		  		Index(graphName).
		      Type("edge").
		  		Do(context.Background())
		  for _, item := range searchResult.Each() {
		    doc := item.(map[string]interface{})
		  }
	*/
	return nil
}

// GetVertexIndexList
func (es *ElasticGraph) GetVertexIndexList() chan aql.IndexID {
	return nil
}

// GetVertexTermCount
func (es *ElasticGraph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	return nil
}

// GetVertexChannel
func (es *ElasticGraph) GetVertexChannel(req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	return nil
}

// GetOutChannel
func (es *ElasticGraph) GetOutChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return nil
}

// GetInChannel
func (es *ElasticGraph) GetInChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return nil
}

// GetOutEdgeChannel
func (es *ElasticGraph) GetOutEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return nil
}

// GetInEdgeChannel
func (es *ElasticGraph) GetInEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return nil
}

// GetOutBundleChannel
func (es *ElasticGraph) GetOutBundleChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return nil
}

// VertexLabelScan
func (es *ElasticGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	return nil
}
