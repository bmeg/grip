package list

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List Data on Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		for g := range conn.GetGraphs() {
			fmt.Printf("%s\n", g)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
}
