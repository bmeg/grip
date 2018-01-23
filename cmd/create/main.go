package create

import (
	//"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host = "localhost:9090"

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "create",
	Short: "Create Graph on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		conn.AddGraph(args[0])
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
}
