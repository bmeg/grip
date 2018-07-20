package schema

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/util/rpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "schema <graph>",
	Short: "Print the schema for a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		jm := jsonpb.Marshaler{
			EnumsAsInts:  false,
			EmitDefaults: true,
			Indent:       "",
			OrigName:     false,
		}

		schema, err := conn.GetSchema(graph)
		if err != nil {
			return err
		}
		txt, err := jm.MarshalToString(schema)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", txt)
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "arachne server url")
}
