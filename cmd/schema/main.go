package schema

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/schema"
	graphSchema "github.com/bmeg/grip/schema"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var yaml = false
var jsonFile string
var yamlFile string
var graphName string
var jsonSchemaFile string
var yamlSchemaDir string
var sampleCount uint32 = 50
var excludeLabels []string

var manual bool

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
			txt, err = graphSchema.GraphToYAMLString(schema)
		} else {
			txt, err = graphSchema.GraphToJSONString(schema)
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
		if jsonFile == "" && yamlFile == "" && jsonSchemaFile == "" && yamlSchemaDir == "" {
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
				graphs, err = graphSchema.ParseJSONGraphs(bytes)
			} else {
				graphs, err = graphSchema.ParseJSONGraphsFile(jsonFile)
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
				graphs, err = graphSchema.ParseYAMLGraphs(bytes)
			} else {
				graphs, err = graphSchema.ParseYAMLGraphsFile(yamlFile)
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

		if jsonSchemaFile != "" && graphName != "" {
			log.Infof("Loading Json Schema file: %s", jsonSchemaFile)
			graphs, err := schema.ParseJSONSchemaGraphsFile(jsonSchemaFile, graphName)
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
		if yamlSchemaDir != "" && graphName != "" {
			log.Infof("Loading Yaml Schema dir: %s", yamlSchemaDir)
			graphs, err := schema.ParseYAMLSchemaGraphsFiles(yamlSchemaDir, graphName)
			if err != nil {
				log.Info("HELLO ERROR HERE: ", err)
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
	pflags.StringVar(&graphName, "graphName", "", "Name of schemaGraph")
	pflags.StringVar(&jsonSchemaFile, "jsonSchema", "", "Json Schema")
	pflags.StringVar(&yamlSchemaDir, "yamlSchemaDir", "", "Name of YAML schemas dir")

	Cmd.AddCommand(getCmd)
	Cmd.AddCommand(postCmd)
}
