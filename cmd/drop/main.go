package drop

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "drop <graph>",
	Short: "Drop a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		return conn.DeleteGraph(args[0])
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
