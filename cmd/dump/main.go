package dump

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
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
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		if vertexDump {
			jm := jsonpb.Marshaler{}
			q := aql.V()
			elems, err := conn.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
			if err != nil {
				return err
			}
			for v := range elems {
				txt, err := jm.MarshalToString(v.GetVertex())
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", txt)
			}
		}

		if edgeDump {
			jm := jsonpb.Marshaler{}
			q := aql.E()
			elems, err := conn.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
			if err != nil {
				return err
			}
			for v := range elems {
				txt, err := jm.MarshalToString(v.GetEdge())
				if err != nil {
					return err
				}
				fmt.Printf("%s\n", txt)
			}
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "arachne server url")
	flags.BoolVar(&vertexDump, "vertex", false, "dump all vertices")
	flags.BoolVar(&edgeDump, "edge", false, "dump all edges")
}
