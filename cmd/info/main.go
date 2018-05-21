package info

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "info <graph>",
	Short: "Info on Arachne Graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		fmt.Printf("Graph: %s\n", graph)

		q := aql.V().Count()
		res, err := conn.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
		if err != nil {
			return err
		}
		for row := range res {
			fmt.Printf("Vertex Count: %s\n", row)
		}

		q = aql.E().Count()
		res, err = conn.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
		if err != nil {
			return err
		}
		for row := range res {
			fmt.Printf("Edge Count: %s\n", row)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Arachne host server")
}
