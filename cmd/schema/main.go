package schema

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/schema"
	"github.com/bmeg/grip/util/rpc"
	"github.com/bmeg/jsonschemagraph/schconv"
	"github.com/spf13/cobra"
	goyaml "gopkg.in/yaml.v3"
)

var host = "localhost:8202"
var yaml = false
var jsonFile string
var yamlFile string
var graphName string
var writeSchema bool = false
var jsonSchemaFile string
var yamlSchemaDir string
var configPath string
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

var loadGqlSchemafromJsonSchema = &cobra.Command{
	Use:   "graphql",
	Short: "Load graph schemas",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonSchemaFile == "" && yamlSchemaDir == "" {
			return fmt.Errorf("no schema file was provided")
		}

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		config := Config{DependencyOrder: []string{}}
		if configPath != "" {
			data, err := ioutil.ReadFile(configPath)
			if err != nil {
				log.Errorf("Failed to read YAML file: %v", err)
			}
			err = goyaml.Unmarshal(data, &config)
			if err != nil {
				log.Errorf("Failed to parse YAML file: %v", err)
			}
		} else {
			fmt.Printf("Warning: No config file was provided, all vertices will be rendered has queries")
		}

		if jsonSchemaFile != "" && graphName != "" {
			log.Infof("Loading Json Schema file: %s", jsonSchemaFile)
			graphs, err := schconv.ParseGraphFile(jsonSchemaFile, "jsonSchema", graphName, config.DependencyOrder, false)
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
			graphs, err := schconv.ParseGraphFile(yamlSchemaDir, "yamlSchema", graphName, config.DependencyOrder, false)
			if err != nil {
				log.Info("HELLO ERROR HERE: ", err)
				return err
			}
			for _, g := range graphs {

				_ = schema.GripGraphqltoGraphql(g, writeSchema)
				//fmt.Println(graphql_string)
				err = conn.AddSchema(g)
				if err != nil {
					return err
				}
				log.Debug("Posted schema: %s", g.Graph)
			}
		}
		return nil
	},
}

var postCmd = &cobra.Command{
	Use:   "post",
	Short: "Post jsonschema graph schemas",
	Long:  ``,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if jsonFile == "" && yamlFile == "" && jsonSchemaFile == "" {
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
		if jsonSchemaFile != "" && graphName != "" {
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
	pflags.StringVar(&graphName, "graphName", "", "Name of schemaGraph")

	gqlflags := loadGqlSchemafromJsonSchema.Flags()
	gqlflags.BoolVar(&writeSchema, "writeSchema", writeSchema, "Write graphql schema to disk")
	gqlflags.StringVar(&host, "host", host, "grip server url")
	gqlflags.StringVar(&jsonSchemaFile, "jsonSchema", "", "Json Schema")
	gqlflags.StringVar(&yamlSchemaDir, "yamlSchemaDir", "", "Name of YAML schemas dir")
	gqlflags.StringVar(&graphName, "graphName", "", "Name of schemaGraph")
	gqlflags.StringVar(&configPath, "configPath", "", "Path of Config file for determining the subset of ")

	Cmd.AddCommand(loadGqlSchemafromJsonSchema)
	Cmd.AddCommand(getCmd)
	Cmd.AddCommand(postCmd)
}
