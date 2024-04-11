package dump

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"
var vertexDump = false
var edgeDump = false
var graph string

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "dump <graph>",
	Short: "Dump vertices/edges from a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph = args[0]
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		if vertexDump {
			q := gripql.V()
			elems, err := conn.Traversal(context.Background(), &gripql.GraphQuery{Graph: graph, Query: q.Statements})
			if err != nil {
				return err
			}
			for v := range elems {
				txt, err := protojson.Marshal(v.GetVertex())
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", string(txt))
			}
		}

		if edgeDump {
			q := gripql.E()
			elems, err := conn.Traversal(context.Background(), &gripql.GraphQuery{Graph: graph, Query: q.Statements})
			if err != nil {
				return err
			}
			for v := range elems {
				txt, err := protojson.Marshal(v.GetEdge())
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", string(txt))
			}
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.BoolVar(&vertexDump, "vertex", false, "dump all vertices")
	flags.BoolVar(&edgeDump, "edge", false, "dump all edges")
}
