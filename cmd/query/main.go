package query

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	gripqljs "github.com/bmeg/grip/gripql/javascript"
	_ "github.com/bmeg/grip/jsengine/goja" // import goja so it registers with the driver map
	_ "github.com/bmeg/grip/jsengine/otto" // import otto so it registers with the driver map
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"
var verbose bool

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

		if verbose {
			c := log.DefaultLoggerConfig()
			c.Level = "debug"
			log.ConfigureLogger(c)
		}

		query, err := gripqljs.ParseQuery(queryString)
		if err != nil {
			log.Errorf("Parse error: %s", err)
			return err
		}
		query.Graph = graph
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			log.Errorf("Connect error: %s", err)
			return err
		}

		log.Debugf("Query: %s\n", query.String())
		res, err := conn.Traversal(context.Background(), query)
		if err != nil {
			log.Errorf("Traversal error: %s", err)
			return err
		}

		count := uint64(0)
		for row := range res {
			rowString, _ := protojson.Marshal(row)
			fmt.Printf("%s\n", rowString)
			count++
		}
		log.Debugf("rows returned: %d", count)
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.BoolVar(&verbose, "verbose", verbose, "Verbose")

}
