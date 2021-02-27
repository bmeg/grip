package query

import (
	"encoding/json"
	"fmt"

	"github.com/bmeg/grip/gripql"
	gripqljs "github.com/bmeg/grip/gripql/javascript"
	_ "github.com/bmeg/grip/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/grip/jsengine/otto" // import otto so it registers with the driver map
	"github.com/bmeg/grip/jsengine/underscore"
	_ "github.com/bmeg/grip/jsengine/v8" // import v8 so it registers with the driver map
	"github.com/bmeg/grip/util/rpc"
	"github.com/dop251/goja"
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
		vm := goja.New()

		us, err := underscore.Asset("underscore.js")
		if err != nil {
			return fmt.Errorf("failed to load underscore.js")
		}
		if _, err := vm.RunString(string(us)); err != nil {
			return err
		}

		gripqlString, err := gripqljs.Asset("gripql.js")
		if err != nil {
			return fmt.Errorf("failed to load gripql.js")
		}
		if _, err := vm.RunString(string(gripqlString)); err != nil {
			return err
		}

		queryString := args[1]
		val, err := vm.RunString(queryString)
		if err != nil {
			return err
		}

		queryJSON, err := json.Marshal(val)
		if err != nil {
			return err
		}

		query := gripql.GraphQuery{}
		err = protojson.Unmarshal(queryJSON, &query)
		if err != nil {
			return err
		}
		query.Graph = args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		res, err := conn.Traversal(&query)
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
