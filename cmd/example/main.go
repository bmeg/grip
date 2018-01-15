
package example


import (
	//"fmt"
  "log"
	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host string = "localhost:9090"
var graph string = "example"
var exampleSet string = "starwars"

var Cmd = &cobra.Command{
	Use:   "example",
	Short: "Load example on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		elemChan := make(chan aql.GraphElement)
		wait := make(chan bool)
		log.Printf("Loading %s into %s", exampleSet, graph)
		go func() {
			conn.StreamElements(elemChan)
			wait <- false
		}()
		for _, vertex := range sw_vertices {
			elemChan <- aql.GraphElement{Graph:graph, Vertex: &vertex}
		}
		for _, edge := range sw_edges {
			elemChan <- aql.GraphElement{Graph:graph, Edge: &edge}
		}
		close(elemChan)
		<- wait
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
  flags.StringVar(&graph, "graph", "example", "Graph")
  flags.StringVar(&exampleSet, "exampleSet", "starwars", "Example Data Set")
}
