package create

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "create <graph>",
	Short: "Create a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		return conn.AddGraph(args[0])
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "arachne server url")
}
