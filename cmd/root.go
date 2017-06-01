package cmd

import (
  "os"
	"github.com/bmeg/arachne/cmd/server"
	"github.com/bmeg/arachne/cmd/rdf"
	"github.com/spf13/cobra"
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
	RootCmd.AddCommand(genBashCompletionCmd)
}


var genBashCompletionCmd = &cobra.Command{
	Use: "bash",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
