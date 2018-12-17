package schema

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var yaml = false

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "schema <graph>",
	Short: "Print the schema for a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		schema, err := conn.GetSchema(graph)
		if err != nil {
			return err
		}

		var txt string
		if yaml {
			txt, err = gripql.SchemaToYAMLString(schema)
		} else {
			txt, err = gripql.SchemaToJSONString(schema)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", txt)
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.BoolVar(&yaml, "yaml", yaml, "output schema in YAML rather than JSON format")
}
