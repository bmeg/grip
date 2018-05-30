package cmd

import (
	"os"

	"github.com/bmeg/arachne/cmd/create"
	"github.com/bmeg/arachne/cmd/drop"
	"github.com/bmeg/arachne/cmd/dump"
	"github.com/bmeg/arachne/cmd/example"
	"github.com/bmeg/arachne/cmd/info"
	"github.com/bmeg/arachne/cmd/list"
	"github.com/bmeg/arachne/cmd/load"
	"github.com/bmeg/arachne/cmd/mongoload"
	"github.com/bmeg/arachne/cmd/query"
	"github.com/bmeg/arachne/cmd/rdf"
	"github.com/bmeg/arachne/cmd/server"
	"github.com/bmeg/arachne/cmd/stream"
	"github.com/bmeg/arachne/cmd/version"
	"github.com/spf13/cobra"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "arachne",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	RootCmd.AddCommand(server.Cmd)
	RootCmd.AddCommand(rdf.Cmd)
	RootCmd.AddCommand(load.Cmd)
	RootCmd.AddCommand(mongoload.Cmd)
	RootCmd.AddCommand(query.Cmd)
	RootCmd.AddCommand(dump.Cmd)
	RootCmd.AddCommand(stream.Cmd)
	RootCmd.AddCommand(create.Cmd)
	RootCmd.AddCommand(drop.Cmd)
	RootCmd.AddCommand(list.Cmd)
	RootCmd.AddCommand(info.Cmd)
	RootCmd.AddCommand(example.Cmd)
	RootCmd.AddCommand(genBashCompletionCmd)
	RootCmd.AddCommand(version.Cmd)
}

var genBashCompletionCmd = &cobra.Command{
	Use:   "bash",
	Short: "Generate bash completions file",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
