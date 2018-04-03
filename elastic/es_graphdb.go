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

func (es *Elastic) Close() {}

func (es *Elastic) GetGraphs() []string {
	graphPrefix := fmt.Sprintf("%s_", es.database)
	out := []string{}
	idxs, err := es.client.IndexNames()
	if err != nil {
		log.Printf("failed to get index names: %s", err)
	}
	for _, c := range idxs {
		if strings.HasPrefix(c, graphPrefix) {
			out = append(out, c[len(graphPrefix):])
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

func (es *Elastic) Graph(graph string) gdbi.GraphInterface {
	return &ElasticGraph{
		url:       es.url,
		database:  es.database,
		ts:        es.ts,
		client:    es.client,
		graph:     graph,
		graphName: fmt.Sprintf("%s_%s", es.database, graph),
	}
}
