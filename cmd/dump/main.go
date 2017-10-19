package dump

import (
	"fmt"
  "log"
	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
  "github.com/golang/protobuf/jsonpb"
)

var host string = "localhost:9090"
var vertexDump bool = false
var edgeDump bool = false
var graph string = "data"

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
      elems, err := conn.Query(graph).V().Execute()
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
      elems, err := conn.Query(graph).E().Execute()
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
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
  flags.StringVar(&graph, "graph", "data", "Graph")
  flags.BoolVar(&vertexDump, "vertex", false, "Dump Vertices")
	flags.BoolVar(&edgeDump, "edge", false, "Dump Edges")
}
