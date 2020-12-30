package server

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/server"
	_ "github.com/go-sql-driver/mysql" //import so mysql will register as a sql driver
	"github.com/imdario/mergo"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
	"github.com/spf13/cobra"
)

var conf = &config.Config{}
var configFile string
var schemaFile string

// Run runs an Grip server.
// This opens a database and starts an API server.
// This blocks indefinitely.
func Run(conf *config.Config, schemas map[string]*gripql.Graph, baseDir string) error {
	log.ConfigureLogger(conf.Logger)
	log.WithFields(log.Fields{"Config": conf}).Info("Starting Server")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	srv, err := server.NewGripServer(conf, schemas, baseDir)
	if err != nil {
		return err
	}

	// Start server
	errch := make(chan error)
	go func() {
		errch <- srv.Serve(ctx)
	}()

	// Block until done.
	// Server must be stopped via the context.
	return <-errch
}

// Cmd the main command called by the cobra library
var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Args:  cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		dconf := config.DefaultConfig()
		if configFile != "" {
			err := config.ParseConfigFile(configFile, dconf)
			if err != nil {
				return fmt.Errorf("error processing config file: %v", err)
			}
		}
		// file vals <- cli val
		err := mergo.MergeWithOverwrite(dconf, conf)
		if err != nil {
			return fmt.Errorf("error processing config file: %v", err)
		}
		conf = dconf

		defaults := config.DefaultConfig()
		if conf.Server.RPCAddress() != defaults.Server.RPCAddress() {
			if conf.Server.RPCAddress() != conf.RPCClient.ServerAddress {
				conf.RPCClient.ServerAddress = conf.Server.RPCAddress()
			}
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		schemaMap := make(map[string]*gripql.Graph)
		if schemaFile != "" {
			schemas, err := gripql.ParseYAMLGraphFile(schemaFile)
			if err != nil {
				return fmt.Errorf("error processing schema file: %v", err)
			}
			for _, s := range schemas {
				schemaMap[s.Graph] = s
			}
		}
		return Run(conf, schemaMap, configFile)
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVarP(&configFile, "config", "c", configFile, "Config file")
	flags.StringVarP(&schemaFile, "schema", "s", schemaFile, "Schema file")
	flags.StringVar(&conf.Server.HTTPPort, "http-port", conf.Server.HTTPPort, "HTTP port")
	flags.StringVar(&conf.Server.RPCPort, "rpc-port", conf.Server.RPCPort, "TCP+RPC port")
	flags.BoolVar(&conf.Server.ReadOnly, "read-only", conf.Server.ReadOnly, "Start server in read-only mode")
	flags.StringVar(&conf.Logger.Level, "log-level", conf.Logger.Level, "Log level [info, debug, warn, error]")
	flags.StringVar(&conf.Logger.Formatter, "log-format", conf.Logger.Formatter, "Log format [text, json]")
	flags.BoolVar(&conf.Server.RequestLogging.Enable, "log-requests", conf.Server.RequestLogging.Enable, "Log all requests")
}
