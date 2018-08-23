package cmd

import (
	"os"

	"github.com/bmeg/grip/cmd/create"
	"github.com/bmeg/grip/cmd/drop"
	"github.com/bmeg/grip/cmd/dump"
	"github.com/bmeg/grip/cmd/info"
	"github.com/bmeg/grip/cmd/list"
	"github.com/bmeg/grip/cmd/load"
	"github.com/bmeg/grip/cmd/mongoload"
	"github.com/bmeg/grip/cmd/query"
	"github.com/bmeg/grip/cmd/rdf"
	"github.com/bmeg/grip/cmd/schema"
	"github.com/bmeg/grip/cmd/server"
	"github.com/bmeg/grip/cmd/stream"
	"github.com/bmeg/grip/cmd/version"
	"github.com/spf13/cobra"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "grip",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	RootCmd.AddCommand(create.Cmd)
	RootCmd.AddCommand(drop.Cmd)
	RootCmd.AddCommand(dump.Cmd)
	RootCmd.AddCommand(genBashCompletionCmd)
	RootCmd.AddCommand(info.Cmd)
	RootCmd.AddCommand(list.Cmd)
	RootCmd.AddCommand(load.Cmd)
	RootCmd.AddCommand(mongoload.Cmd)
	RootCmd.AddCommand(query.Cmd)
	RootCmd.AddCommand(rdf.Cmd)
	RootCmd.AddCommand(schema.Cmd)
	RootCmd.AddCommand(server.Cmd)
	RootCmd.AddCommand(stream.Cmd)
	RootCmd.AddCommand(version.Cmd)
}

var genBashCompletionCmd = &cobra.Command{
	Use:   "bash",
	Short: "Generate bash completions file",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
