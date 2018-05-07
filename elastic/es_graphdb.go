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

// Config describes the configuration for the elasticsearch driver.
type Config struct {
	URL         string
	DBName      string
	Synchronous bool
	BatchSize   int
}

// Elastic implements the GraphDB interface with elasticsearch as a backend
type Elastic struct {
	database string
	conf     Config
	ts       *timestamp.Timestamp
	client   *elastic.Client
}

// NewElastic creates a new elasticsearch graph database interface
func NewElastic(conf Config) (gdbi.GraphDB, error) {
	log.Printf("Starting Elastic Driver")
	database := strings.ToLower(conf.DBName)
	if strings.ContainsAny(database, `/\. "'$*<>:|?`) {
		return nil, fmt.Errorf(`invalid database name; cannot contain /\. "'$*<>:|?`)
	}
	if strings.HasPrefix(database, "_") || strings.HasPrefix(database, "+") || strings.HasPrefix(database, "-") {
		return nil, fmt.Errorf(`invalid database name; cannot start with _-+`)
	}

	ts := timestamp.NewTimestamp()
	client, err := elastic.NewClient(
		elastic.SetURL(conf.URL),
		elastic.SetSniff(false),
		elastic.SetRetrier(
			elastic.NewBackoffRetrier(
				elastic.NewExponentialBackoff(time.Millisecond*50, time.Minute),
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %v", err)
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	db := &Elastic{database: database, conf: conf, ts: &ts, client: client}
	for _, i := range db.GetGraphs() {
		db.ts.Touch(i)
	}
	return db, nil
}

// Close closes connection to elastic search
func (es *Elastic) Close() error {
	es.client.Stop()
	return nil
}

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

	if strings.ContainsAny(graph, `/\. "'$*<>:|?`) {
		return fmt.Errorf(`invalid graph name; cannot contain /\. "'$*<>:|?`)
	}
	if strings.HasPrefix(graph, "_") || strings.HasPrefix(graph, "+") || strings.HasPrefix(graph, "-") {
		return fmt.Errorf(`invalid graph name; cannot start with _-+`)
	}

	vertexIndex := fmt.Sprintf("%s_%s_vertex", es.database, graph)
	vMapping := `{
    "mappings": {
      "vertex":{
        "properties":{
          "gid": {
            "type": "keyword"
          },
          "label": {
            "type": "keyword"
          }
        }
      }
    }
  }`
	if err := es.initIndex(ctx, vertexIndex, vMapping); err != nil {
		return err
	}

	edgeIndex := fmt.Sprintf("%s_%s_edge", es.database, graph)
	eMapping := `{
    "mappings": {
      "edge":{
        "properties":{
          "gid": {
            "type": "keyword"
          },
          "from": {
            "type": "keyword"
          },
          "to": {
            "type": "keyword"
          },
          "label": {
            "type": "keyword"
          }
        }
      }
    }
  }`
	if err := es.initIndex(ctx, edgeIndex, eMapping); err != nil {
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
func (es *Elastic) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range es.GetGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}

	return &Graph{
		ts:          es.ts,
		client:      es.client,
		graph:       graph,
		vertexIndex: fmt.Sprintf("%s_%s_vertex", es.database, graph),
		edgeIndex:   fmt.Sprintf("%s_%s_edge", es.database, graph),
		batchSize:   es.conf.BatchSize,
		synchronous: es.conf.Synchronous,
	}, nil
}
