package plugin

import (
	//"context"
	"fmt"
	"github.com/bmeg/grip/gripql"
	"github.com/spf13/cobra"
  "github.com/bmeg/grip/util/rpc"
	"github.com/bmeg/grip/log"
	//"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"

var startConfig map[string]string

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
		if err == nil {
	    for _, l := range lst.Drivers {
	      fmt.Printf("%s\n", l)
	    }
		} else {
			log.Error(err)
		}
		return err
	},
}


var ListPluginsCmd = &cobra.Command{
	Use:   "list-plugins",
	Short: "Get info about a collection",
	RunE: func(cmd *cobra.Command, args []string) error {
    conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
    if err != nil {
      return err
    }
		conn = conn.WithConfigureAPI()
    lst, err := conn.ListPlugins()
		if err == nil {
	    for _, l := range lst.Plugins {
	      fmt.Printf("%s\n", l)
	    }
		} else {
			log.Error(err)
		}
		return err
	},
}

var StartPluginCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a plugin driver",
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		driver := args[0]
		name := args[1]
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
    if err != nil {
      return err
    }
		conn = conn.WithConfigureAPI()
		_, err = conn.StartPlugin( &gripql.PluginConfig{Driver: driver, Name: name, Config:startConfig} )
		return err
	},
}


func init() {
	StartPluginCmd.Flags().StringToStringVarP(&startConfig, "config", "c", startConfig, "plugin params")

	Cmd.AddCommand(ListDriversCmd)
	Cmd.AddCommand(ListPluginsCmd)
	Cmd.AddCommand(StartPluginCmd)
}
