package example

import (
	"fmt"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/golang/protobuf/jsonpb"
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

		graphql := fmt.Sprintf("%s:schema", graph)

		graphs := conn.GetGraphList()
		if !found(graphs, graphql) {
			conn.AddGraph(graphql)
		}
		if !found(graphs, graph) {
			conn.AddGraph(graph)
		}

		elemChan := make(chan *aql.GraphElement)
		wait := make(chan bool)
		log.Printf("Loading %s into %s", exampleSet, graph)
		go func() {
			conn.StreamElements(elemChan)
			wait <- false
		}()
		for _, vertex := range swVertices {
			v := vertex
			elemChan <- &aql.GraphElement{Graph: graph, Vertex: &v}
		}
		for _, edge := range swEdges {
			e := edge
			elemChan <- &aql.GraphElement{Graph: graph, Edge: &e}
		}
		close(elemChan)
		<-wait

		e := aql.Graph{}
		if err := jsonpb.Unmarshal(strings.NewReader(swGQLGraph), &e); err != nil {
			log.Printf("Error: %s", err)
		}
		conn.AddSubGraph(graphql, &e)

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "example", "Graph")
	flags.StringVar(&exampleSet, "exampleSet", "starwars", "Example Data Set")
}
