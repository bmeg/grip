package query

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	gripqljs "github.com/bmeg/grip/gripql/javascript"
	_ "github.com/bmeg/grip/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/grip/jsengine/otto" // import otto so it registers with the driver map
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "query <graph> <query expression>",
	Short: "Query a graph",
	Long: `Query a graph.
Example:
    grip query example-graph 'V().hasLabel("Variant").out().limit(5)'`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]
		queryString := args[1]

		query, err := gripqljs.ParseQuery(queryString)
		if err != nil {
			return err
		}
		query.Graph = graph
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		res, err := conn.Traversal(query)
		if err != nil {
			return err
		}

		for row := range res {
			rowString, _ := protojson.Marshal(row)
			fmt.Printf("%s\n", rowString)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
