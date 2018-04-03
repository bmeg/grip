package elastic

import (
	"context"
	"fmt"
	"strings"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/olivere/elastic"
  "github.com/bmeg/arachne/engine/core"
	"log"
)

type Elastic struct {
	url      string
	database string
	ts       *timestamp.Timestamp
	client   *elastic.Client
}

// NewElastic creates a new elastic search graph database interface
func NewElastic(url string, database string) gdbi.GraphDB {
	log.Printf("Starting Elastic Driver")
	ts := timestamp.NewTimestamp()
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		log.Printf("%s", err)
	}
	a := &Elastic{url: url, database: database, ts: &ts, client: client}
	for _, i := range a.GetGraphs() {
		a.ts.Touch(i)
	}
	return a
}

func (es *Elastic) Close() {}

func (es *Elastic) GetGraphs() []string {
	graphPrefix := fmt.Sprintf("%s_", es.database)
	out := []string{}
	idxs, _ := es.client.IndexNames()
	for _, c := range idxs {
		if strings.HasPrefix(c, graphPrefix) {
			out := append(out, c[len(graphPrefix):])
		}
	}
	return out
}

func (es *Elastic) AddGraph(graph string) error {
	graphName := fmt.Sprintf("%s_%s", es.database, graph)
	_, err := es.client.CreateIndex(graphName).Do(context.Background())
	return err
}

func (es *Elastic) DeleteGraph(graph string) error {
	graphName := fmt.Sprintf("%s_%s", es.database, graph)
	_, err := es.client.DeleteIndex(graphName).Do(context.Background())
	return err
}

type ElasticGraph struct {
	url      string
	database string
	ts       *timestamp.Timestamp
	client   *elastic.Client
	graph    string
}

func (es *Elastic) Graph(graph string) gdbi.GraphInterface {
	return &ElasticGraph{
		url:      es.url,
		database: es.database,
		ts:       es.ts,
		client:   es.client,
		graph:    graph,
	}
}

func (es *ElasticGraph) AddBundle(bundle *aql.Bundle) error {
	return nil
}

func (es *ElasticGraph) DelBundle(eid string) error {
  return nil
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddEdge(edgeArray []*aql.Edge) error {
  bulkRequest := es.client.Bulk()
  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
  for _, e := range edgeArray {
    edoc := PackEdge(e)
    req := elastic.NewBulkIndexRequest().Index(graphName).Type("edge").Id(e.Gid).Doc(edoc)
		bulkRequest = bulkRequest.Add(req)
  }
  _, err := bulkRequest.Do(context.Background())
  return err
}

// AddVertex adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddVertex(vertexArray []*aql.Vertex) error {
  bulkRequest := es.client.Bulk()
  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
  for _, e := range vertexArray {
    edoc := PackVertex(e)
    req := elastic.NewBulkIndexRequest().Index(graphName).Type("vertex").Id(e.Gid).Doc(edoc)
		bulkRequest = bulkRequest.Add(req)
  }
  _, err := bulkRequest.Do(context.Background())
  return err
}

// DelEdge
func (es *ElasticGraph) DelEdge(eid string) error {
  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
  _, err := es.client.Delete().Index(graphName).Type("edge").Id(eid).Do(context.Background())
  return err
}

// DelEdge
func (es *ElasticGraph) DelVertex(vid string) error {
  //TODO: remove connected edges
  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
  _, err := es.client.Delete().Index(graphName).Type("vertex").Id(vid).Do(context.Background())
  return err
}

// Compiler
func (es *ElasticGraph) Compiler() gdbi.Compiler {
	return core.NewCompiler(es)
}

func (es *ElasticGraph) GetBundle(id string, loadProp bool) *aql.Bundle {
  return nil
}

func (es *ElasticGraph) GetEdge(id string, loadProp bool) *aql.Edge {
  graphName := fmt.Sprintf("%s_%s", es.database, es.graph)
  get1, err := es.Client.Get().Index(graphName).Type("vertex").Id(id).Do(context.Background())
  edge := UnpackEdge(get1.Field)
  return edge
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
