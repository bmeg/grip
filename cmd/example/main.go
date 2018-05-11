package example

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var graph = "example"
var exampleSet = "starwars"

func found(set []string, val string) bool {
	for _, i := range set {
		if i == val {
			return true
		}
	}
	return false
}

// Cmd is the example loader command line definition
var Cmd = &cobra.Command{
	Use:   "example",
	Short: "Load example on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		graphql := fmt.Sprintf("%s-schema", graph)

		graphs := conn.GetGraphList()
		if !found(graphs, graphql) {
			conn.AddGraph(graphql)
		}
		if !found(graphs, graph) {
			conn.AddGraph(graph)
		}

		log.Printf("Loading example graph data into %s", graph)
		elemChan := make(chan *aql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %s", err)
			}
			wait <- false
		}()

		log.Printf("Loading example graphql schema into %s", graphql)
		schema := &aql.Graph{}
		if err := json.Unmarshal([]byte(swGQLGraph), schema); err != nil {
			return fmt.Errorf("failed to unmarshal graph schema: %v", err)
		}
		for _, v := range schema.Vertices {
			elemChan <- &aql.GraphElement{Graph: graphql, Vertex: v}
		}
		for _, e := range schema.Edges {
			elemChan <- &aql.GraphElement{Graph: graphql, Edge: e}
		}

		close(elemChan)
		<-wait

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "example", "Graph")
}
