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
var file string

type Data struct {
	Graph    string   `json:"graph"`
	Edges    []string `json:"edges"`
	Vertices []string `json:"vertices"`
}

// Cmd command line declaration
var Cmd = &cobra.Command{
	Use:   "delete <graph elem list file>",
	Short: "bulk delete",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		file = args[0]
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

		log.WithFields(log.Fields{"graph": data.Graph}).Info("deleting data")
		conn.BulkDelete(&gripql.DeleteData{Graph: data.Graph, Vertices: data.Vertices, Edges: data.Edges})

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.StringVar(&file, "file", file, "file name")
}
