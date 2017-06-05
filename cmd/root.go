package cmd

import (
	"github.com/bmeg/arachne/cmd/load"
	"github.com/bmeg/arachne/cmd/rdf"
	"github.com/bmeg/arachne/cmd/server"
	"github.com/spf13/cobra"
	"os"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "funnel",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	RootCmd.AddCommand(server.Cmd)
	RootCmd.AddCommand(rdf.Cmd)
	RootCmd.AddCommand(load.Cmd)
	RootCmd.AddCommand(genBashCompletionCmd)
}

var genBashCompletionCmd = &cobra.Command{
	Use: "bash",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
