package dump

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"log"
)

var host = "localhost:8202"
var vertexDump = false
var edgeDump = false
var graph = "data"

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump Data on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		if vertexDump {
			jm := jsonpb.Marshaler{}
      q := aql.NewQuery(graph).V()
			elems, err := conn.Execute(q)
			if err != nil {
				log.Printf("ERROR: %s", err)
				return err
			}
			for v := range elems {
				txt, _ := jm.MarshalToString(v.Value.GetVertex())
				fmt.Printf("%s\n", txt)
			}
		}

		if edgeDump {
			jm := jsonpb.Marshaler{}
      q := aql.NewQuery(graph).E()
			elems, err := conn.Execute(q)
			if err != nil {
				log.Printf("ERROR: %s", err)
				return err
			}
			for v := range elems {
				txt, _ := jm.MarshalToString(v.Value.GetEdge())
				fmt.Printf("%s\n", txt)
			}
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "data", "Graph")
	flags.BoolVar(&vertexDump, "vertex", false, "Dump Vertices")
	flags.BoolVar(&edgeDump, "edge", false, "Dump Edges")
}
