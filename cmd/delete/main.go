package delete

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var file string
var edges []string
var vertices []string
var graph string
var data Data

type Data struct {
	Graph    string   `json:"graph"`
	Edges    []string `json:"edges"`
	Vertices []string `json:"vertices"`
}

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "delete <graph>",
	Short: "Delete data from a graph",
	Long: `JSON File Format: {
	"graph": 'graph_name',
	"edges":['list of edge ids'],
	"vertices":['list of vertice ids']
}

comma delimited --edges or --vertices arguments are also supported ex:
--edges="edge1,edge2"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if file == "" && edges == nil && vertices == nil {
			return fmt.Errorf("no input file path or --edges or --vertices arg was provided")
		}

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		graph = args[0]

		if file != "" {
			jsonFile, err := os.Open(file)
			if err != nil {
				log.Errorf("Failed to open file: %s", err)
			}
			defer jsonFile.Close()

			// Read the JSON file
			byteValue, err := ioutil.ReadAll(jsonFile)
			if err != nil {
				log.Errorf("Failed to read file: %s", err)
			}

			// Unmarshal the JSON into the Data struct
			err = json.Unmarshal(byteValue, &data)
			if err != nil {
				log.Errorf("Failed to unmarshal JSON: %s", err)
			}
		} else if edges != nil || vertices != nil {
			data.Edges = edges
			data.Vertices = vertices
		}

		log.WithFields(log.Fields{"graph": graph}).Info("deleting data")
		log.Info("VALUE OF DATA: ", data.Edges, data.Vertices)
		conn.BulkDelete(&gripql.DeleteData{Graph: graph, Vertices: data.Vertices, Edges: data.Edges})

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.StringSliceVar(&edges, "edges", edges, "grip edges list")
	flags.StringSliceVar(&vertices, "vertices", vertices, "grip vertices list")
	flags.StringVar(&file, "file", file, "file name")
}
