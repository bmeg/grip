package example

import (
	"fmt"
	"log"

	"github.com/bmeg/grip/aql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var graph = "example-graph"
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
	Use:   "example-graph",
	Short: "Load an example graph",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		graphCh, err := conn.ListGraphs()
		if err != nil {
			return err
		}
		graphs := []string{}
		for g := range graphCh {
			graphs = append(graphs, g)
		}
		if found(graphs, graph) {
			return fmt.Errorf("grip already contains a graph called %s", graph)
		}

		err = conn.AddGraph(graph)
		if err != nil {
			return err
		}

		elemChan := make(chan *aql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %s", err)
			}
			wait <- false
		}()

		log.Printf("Loading example graph data into %s", graph)
		for _, v := range swVertices {
			elemChan <- &aql.GraphElement{Graph: graph, Vertex: v}
		}
		for _, e := range swEdges {
			elemChan <- &aql.GraphElement{Graph: graph, Edge: e}
		}

		close(elemChan)
		<-wait

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
