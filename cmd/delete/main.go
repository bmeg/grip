package delete

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var graph = "GEN3"
var file string

type Data struct {
	Edges    []string `json:"EDGES"`
	Vertices []string `json:"VERTICES"`
}

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "delete <graph elem list file>",
	Short: "bulk delete",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		graph = args[0]
		file = args[1]
		if file == "" {
			log.Errorln("No input file found")
		}

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
		var data Data
		err = json.Unmarshal(byteValue, &data)
		if err != nil {
			log.Errorf("Failed to unmarshal JSON: %s", err)
		}
		conn.BulkDelete(&gripql.DeleteData{Graph: graph, Vertices: data.Vertices, Edges: data.Edges})

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.StringVar(&graph, "graph", graph, "graph name")
	flags.StringVar(&file, "file", file, "file name")
}
