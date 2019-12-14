package cmd

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // enable pprof via a flag
	"os"

	"github.com/bmeg/grip/cmd/create"
	"github.com/bmeg/grip/cmd/drop"
	"github.com/bmeg/grip/cmd/dump"
	"github.com/bmeg/grip/cmd/info"
	"github.com/bmeg/grip/cmd/kvload"
	"github.com/bmeg/grip/cmd/list"
	"github.com/bmeg/grip/cmd/load"
	"github.com/bmeg/grip/cmd/mongoload"
	"github.com/bmeg/grip/cmd/query"
	"github.com/bmeg/grip/cmd/rdf"
	"github.com/bmeg/grip/cmd/schema"
	"github.com/bmeg/grip/cmd/server"
	"github.com/bmeg/grip/cmd/stream"
	"github.com/bmeg/grip/cmd/version"
	"github.com/bmeg/grip/cmd/index"
	"github.com/bmeg/grip/log"
	"github.com/spf13/cobra"
)

var enableProf bool

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "grip",
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if enableProf {
			go func() {
				err := http.ListenAndServe(":6060", nil)
				if err != nil {
					panic(fmt.Errorf("pprof error: %v", err))
				}
			}()
			log.Infoln("pprof listening on :6060")
		}
	},
}

func init() {
	RootCmd.PersistentFlags().BoolVar(&enableProf, "pprof", enableProf, "enable pprof on port 6060")
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
	RootCmd.AddCommand(kvload.Cmd)
	RootCmd.AddCommand(index.Cmd)
}

var genBashCompletionCmd = &cobra.Command{
	Use:   "bash",
	Short: "Generate bash completions file",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
