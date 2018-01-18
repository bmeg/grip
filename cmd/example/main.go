package example

import (
	//"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
	"log"
)

var host string = "localhost:9090"
var graph string = "example"
var exampleSet string = "starwars"

func found(set []string, val string) bool {
	for _, i := range set {
		if i == val {
			return true
		}
	}
	return false
}

var Cmd = &cobra.Command{
	Use:   "example",
	Short: "Load example on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		graphs := conn.GetGraphList()

		if !found(graphs, "graphql") {
			conn.AddGraph("graphql")
		}
		if !found(graphs, graph) {
			conn.AddGraph(graph)
		}

		elemChan := make(chan aql.GraphElement)
		wait := make(chan bool)
		log.Printf("Loading %s into %s", exampleSet, graph)
		go func() {
			conn.StreamElements(elemChan)
			wait <- false
		}()
		for _, vertex := range sw_vertices {
			v := vertex
			elemChan <- aql.GraphElement{Graph: graph, Vertex: &v}
		}
		for _, edge := range sw_edges {
			e := edge
			elemChan <- aql.GraphElement{Graph: graph, Edge: &e}
		}

		for _, vertex := range sw_gql_vertices {
			v := vertex
			elemChan <- aql.GraphElement{Graph: "graphql", Vertex: &v}
		}
		for _, edge := range sw_gql_edges {
			e := edge
			elemChan <- aql.GraphElement{Graph: "graphql", Edge: &e}
		}

		close(elemChan)
		<-wait
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
	flags.StringVar(&graph, "graph", "example", "Graph")
	flags.StringVar(&exampleSet, "exampleSet", "starwars", "Example Data Set")
}
