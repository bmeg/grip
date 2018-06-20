package list

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List graphs",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		graphs, err := conn.ListGraphs()
		if err != nil {
			return err
		}
		for g := range graphs {
			fmt.Printf("%s\n", g)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "arachne server url")
}
