package info

import (
	"fmt"

	"github.com/bmeg/arachne/aql"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "info",
	Short: "Info on Arachne Graph",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return cmd.Usage()
		}

		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}
		fmt.Printf("Graph: %s\n", args[0])

		q := aql.V().Count()
		res, _ := conn.Execute(args[0], q)
		for row := range res {
			fmt.Printf("Vertex Count: %s\n", row)
		}
		q = aql.E().Count()
		res, _ = conn.Execute(args[0], q)
		for row := range res {
			fmt.Printf("Edge Count: %s\n", row)
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
}
