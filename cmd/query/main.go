package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bmeg/grip/aql"
	aqljs "github.com/bmeg/grip/aql/javascript"
	_ "github.com/bmeg/grip/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/grip/jsengine/otto" // import otto so it registers with the driver map
	"github.com/bmeg/grip/jsengine/underscore"
	_ "github.com/bmeg/grip/jsengine/v8" // import v8 so it registers with the driver map
	"github.com/bmeg/grip/util/rpc"
	"github.com/dop251/goja"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "query <graph> <query expression>",
	Short: "Query a graph",
	Long: `Query a graph. 
Example:
    grip query example-graph 'V().where(eq("_label", "Variant")).out().limit(5)'`,
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

		aqlString, err := aqljs.Asset("aql.js")
		if err != nil {
			return fmt.Errorf("failed to load underscore.js")
		}
		if _, err := vm.RunString(string(aqlString)); err != nil {
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

		query := aql.GraphQuery{}
		err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
		if err != nil {
			return err
		}
		query.Graph = args[0]

		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		res, err := conn.Traversal(&query)
		if err != nil {
			return err
		}

		marsh := jsonpb.Marshaler{}
		for row := range res {
			rowString, _ := marsh.MarshalToString(row)
			fmt.Printf("%s\n", rowString)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
