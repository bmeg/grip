package info

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "info <graph>",
	Short: "Print vertex/edge counts for a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		fmt.Printf("Graph: %s\n", graph)

		q := gripql.V().Count()
		res, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
		if err != nil {
			return err
		}
		for row := range res {
			fmt.Printf("Vertex Count: %v\n", row.GetCount())
		}

		q = gripql.E().Count()
		res, err = conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
		if err != nil {
			return err
		}
		for row := range res {
			fmt.Printf("Edge Count: %v\n", row.GetCount())
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
