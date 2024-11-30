package jsonload

import (
	"encoding/json"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var NdJsonFile string
var workerCount = 5
var graph string
var ExtraArgs string
var logRate = 10000

var Cmd = &cobra.Command{
	Use:   "jsonload <NdJsonFile> <graph> <ExtraArgs>",
	Short: "Load, Validate NdJson data into grip graph",
	Long:  ``,
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		NdJsonFile = args[0]
		graph = args[1]
		ExtraArgs = args[2]

		var args_map map[string]any
		if ExtraArgs != "" {
			err := json.Unmarshal([]byte(ExtraArgs), &args_map)
			if err != nil {
				return err
			}
		}

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		resp, err := conn.ListGraphs()
		if err != nil {
			return err
		}
		found := false
		for _, g := range resp.Graphs {
			if graph == g {
				found = true
			}
		}
		if !found {
			log.WithFields(log.Fields{"graph": graph}).Info("creating graph")
			err := conn.AddGraph(graph)
			if err != nil {
				return err
			}
		}
		elemChan := make(chan *gripql.RawJson)
		wait := make(chan bool)
		go func() {
			_, err := conn.BulkAddRaw(elemChan)
			if err != nil {
				log.Errorf("bulk add raw error: %v", err)
			}
			wait <- false
		}()

		jsonChan, err := util.StreamRawJsonFromFile(NdJsonFile, workerCount, graph, args_map)
		if err != nil {
			return err
		}
		count := 0
		for j := range jsonChan {
			count++
			if count%logRate == 0 {
				log.Infof("Loaded %d vertices", count)
			}
			elemChan <- j
		}

		close(elemChan)
		<-wait

		log.WithFields(log.Fields{"graph": graph}).Info("loading data")
		return nil
	},
}
