package schema

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/schema"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var yaml = false
var jsonFile string
var yamlFile string
var graphName string
var jsonSchemaFile string
var yamlSchemaPath string

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

		gripqlschema, err := conn.GetSchema(graph)
		if err != nil {
			return err
		}

		var txt string
		if yaml {
			txt, err = schema.GraphToYAMLString(gripqlschema)
		} else {
			txt, err = schema.GraphToJSONString(gripqlschema)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", txt)
		return nil
	},
}

type Config struct {
	DependencyOrder []string `yaml:"dependency_order"`
}

var postCmd = &cobra.Command{
	Use:   "post [graph name]",
	Short: "Post jsonschema graph schemas",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graphName := args[0]
		if jsonFile == "" && yamlFile == "" && jsonSchemaFile == "" && yamlSchemaPath == "" {
			return fmt.Errorf("no schema file was provided")
		}

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		if jsonFile != "" {
			var graphs []*gripql.Graph
			var err error
			if jsonFile == "-" {
				bytes, err := ioutil.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				graphs, err = schema.ParseJSONGraphs(bytes)
			} else {
				graphs, err = schema.ParseJSONGraphsFile(jsonFile)
			}
			if err != nil {
				return err
			}
			for _, g := range graphs {
				err := conn.AddSchema(g)
				if err != nil {
					return err
				}
				log.Debug("Posted schema: %s", g.Graph)
			}
		}

		if yamlFile != "" {
			var graphs []*gripql.Graph
			var err error
			if jsonFile == "-" {
				bytes, err := ioutil.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				graphs, err = schema.ParseYAMLGraphs(bytes)
			} else {
				graphs, err = schema.ParseYAMLGraphsFile(yamlFile)
			}
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
		if jsonSchemaFile != "" {
			log.Infof("Loading Json Schema file: %s", jsonSchemaFile)
			graphs, err := schema.ParseJsonSchema(jsonSchemaFile, graphName)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				err := conn.AddSchema(g)
				if err != nil {
					return err
				}
				log.Debug("Posted schema: %s", g.Graph)
			}
		}
		if yamlSchemaPath != "" {
			log.Infof("Loading Yaml Schema path: %s", yamlSchemaPath)
			graphs, err := schema.ParseYamlSchemaPath(yamlSchemaPath, graphName)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				err := conn.AddSchema(g)
				if err != nil {
					return err
				}
				log.Debug("Posted schema: %s", g.Graph)
			}
		}
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
	pflags.StringVar(&jsonSchemaFile, "jsonSchema", "", "Json Schema")
	pflags.StringVar(&yamlSchemaPath, "yamlSchemaPath", "", "Name of the dir that contains YAML schemas")

	Cmd.AddCommand(getCmd)
	Cmd.AddCommand(postCmd)
}
