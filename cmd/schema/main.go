package schema

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	gripql_schema "github.com/bmeg/grip/gripql/schema"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var yaml = false
var jsonFile string
var yamlFile string
var sampleCount uint32 = 50
var excludeLabels []string

// Cmd line declaration
var Cmd = &cobra.Command{
	Use:   "schema",
	Short: "Graph schema operations",
}

var getCmd = &cobra.Command{
	Use:   "get <graph>",
	Short: "Get the schema for a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		schema, err := conn.GetSchema(graph)
		if err != nil {
			return err
		}

		var txt string
		if yaml {
			txt, err = gripql.GraphToYAMLString(schema)
		} else {
			txt, err = gripql.GraphToJSONString(schema)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", txt)
		return nil
	},
}

var postCmd = &cobra.Command{
	Use:   "post",
	Short: "Post graph schemas",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonFile == "" && yamlFile == "" {
			return fmt.Errorf("no schema file was provided")
		}

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		if jsonFile != "" {
			graphs, err := gripql.ParseJSONGraphFile(jsonFile)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				err := conn.AddSchema(g)
				if err != nil {
					return err
				}
			}
		}

		if yamlFile != "" {
			graphs, err := gripql.ParseYAMLGraphFile(yamlFile)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				err := conn.AddSchema(g)
				if err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var sampleCmd = &cobra.Command{
	Use:   "sample <graph>",
	Short: "Sample graph and construct schema",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph := args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		schema, err := gripql_schema.ScanSchema(conn, graph, sampleCount, excludeLabels)
		if err != nil {
			return err
		}

		var txt string
		if yaml {
			txt, err = gripql.GraphToYAMLString(schema)
		} else {
			txt, err = gripql.GraphToJSONString(schema)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", txt)
		conn.Close()
		return nil
	},
}

func init() {
	gflags := getCmd.Flags()
	gflags.StringVar(&host, "host", host, "grip server url")
	gflags.BoolVar(&yaml, "yaml", yaml, "output schema in YAML rather than JSON format")

	pflags := postCmd.Flags()
	pflags.StringVar(&host, "host", host, "grip server url")
	pflags.StringVar(&jsonFile, "json", "", "JSON graph file")
	pflags.StringVar(&yamlFile, "yaml", "", "YAML graph file")

	sflags := sampleCmd.Flags()
	sflags.StringVar(&host, "host", host, "grip server url")
	sflags.Uint32Var(&sampleCount, "sample", sampleCount, "Number of elements to sample")
	sflags.BoolVar(&yaml, "yaml", yaml, "output schema in YAML rather than JSON format")
	sflags.StringSliceVar(&excludeLabels, "exclude-label", excludeLabels, "exclude vertex/edge label from schema")

	Cmd.AddCommand(getCmd)
	Cmd.AddCommand(postCmd)
	Cmd.AddCommand(sampleCmd)
}
