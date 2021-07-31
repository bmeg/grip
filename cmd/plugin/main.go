package plugin

import (
	//"context"
	"fmt"
	"github.com/bmeg/grip/gripql"
	"github.com/spf13/cobra"
  "github.com/bmeg/grip/util/rpc"
	//"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "plugin",
	Short: "Issue command to plugin control",
}

var ListDriversCmd = &cobra.Command{
	Use:   "list-drivers",
	Short: "Get info about a collection",
	RunE: func(cmd *cobra.Command, args []string) error {
    conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
    if err != nil {
      return err
    }
		conn = conn.WithConfigureAPI()
    lst, err := conn.ListDrivers()
    for _, l := range lst.Drivers {
      fmt.Printf("%s\n", l)
    }
		return err
	},
}

var StartPluginCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a plugin driver",
	RunE: func(cmd *cobra.Command, args []string) error {
		//client := getClient()
		return nil
	},
}


func init() {
	Cmd.AddCommand(ListDriversCmd)
}
