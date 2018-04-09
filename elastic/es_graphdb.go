package elastic

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	elastic "gopkg.in/olivere/elastic.v5"
)

// Elastic implements the GraphDB interface with elastic search as a backend
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
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetRetrier(
			elastic.NewBackoffRetrier(
				elastic.NewExponentialBackoff(time.Millisecond*50, time.Minute),
			),
		),
	)
	if err != nil {
		log.Printf("failed to create elastic client: %s", err)
	}
	gdb := &Elastic{url: url, database: database, ts: &ts, client: client}
	for _, i := range gdb.GetGraphs() {
		gdb.ts.Touch(i)
	}
	return gdb
}

// Close closes connection to elastic search
func (es *Elastic) Close() {}

// GetGraphs returns list of graphs on elastic search instance
func (es *Elastic) GetGraphs() []string {
	graphPrefix := fmt.Sprintf("%s_", es.database)
	out := []string{}
	idxNames, err := es.client.IndexNames()
	if err != nil {
		log.Printf("failed to get index names: %s", err)
	}
	for _, idx := range idxNames {
		if strings.HasPrefix(idx, graphPrefix) {
			gName := strings.TrimPrefix(idx, graphPrefix)
			gName = strings.TrimSuffix(gName, "_vertex")
			gName = strings.TrimSuffix(gName, "_edge")
			out = append(out, gName)
		}
	}
	return out
}

func (es *Elastic) initIndex(ctx context.Context, name, body string) error {
	exists, err := es.client.
		IndexExists(name).
		Do(ctx)

	if err != nil {
		return err
	} else if !exists {
		if _, err := es.client.CreateIndex(name).Body(body).Do(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AddGraph adds a new graph to the graphdb
func (es *Elastic) AddGraph(graph string) error {
	ctx := context.Background()
	vertexIndex := fmt.Sprintf("%s_%s_vertex", es.database, graph)
	if err := es.initIndex(ctx, vertexIndex, ""); err != nil {
		return err
	}

	edgeIndex := fmt.Sprintf("%s_%s_edge", es.database, graph)
	if err := es.initIndex(ctx, edgeIndex, ""); err != nil {
		return err
	}
	return nil
}

// DeleteGraph deletes a graph from the graphdb
func (es *Elastic) DeleteGraph(graph string) error {
	ctx := context.Background()
	vertexIndex := fmt.Sprintf("%s_%s_vertex", es.database, graph)
	if _, err := es.client.DeleteIndex(vertexIndex).Do(ctx); err != nil {
		return err
	}

	edgeIndex := fmt.Sprintf("%s_%s_edge", es.database, graph)
	if _, err := es.client.DeleteIndex(edgeIndex).Do(ctx); err != nil {
		return err
	}
	return nil
}

// Graph returns interface to a specific graph in the graphdb
func (es *Elastic) Graph(graph string) gdbi.GraphInterface {
	// TODO pass config to down to the Graph instance
	return &Graph{
		url:         es.url,
		database:    es.database,
		ts:          es.ts,
		client:      es.client,
		graph:       graph,
		vertexIndex: fmt.Sprintf("%s_%s_vertex", es.database, graph),
		edgeIndex:   fmt.Sprintf("%s_%s_edge", es.database, graph),
		batchSize:   1000,
		synchronous: true,
	}
}
