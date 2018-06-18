package drop

import (
	"github.com/bmeg/arachne/aql"
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
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		return conn.DeleteGraph(args[0])
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "arachne server url")
}
