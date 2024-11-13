package caliperload

import (
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
)

const GRAPH = "CALIPER"

var host = "localhost:8202"
var NdJsonFile string
var workerCount = 1
var logRate = 10000

var Cmd = &cobra.Command{
	Use:   "caliperload <NdJsonFile>",
	Short: "Load, Validate NdJson data into Caliper graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		NdJsonFile = args[0]
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
			if GRAPH == g {
				found = true
			}
		}
		if !found {
			log.WithFields(log.Fields{"graph": GRAPH}).Info("creating graph")
			err := conn.AddGraph(GRAPH)
			if err != nil {
				return err
			}
		}
		elemChan := make(chan *gripql.RawJson)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAddRaw(elemChan); err != nil {
				log.Errorf("bulk add error: %v", err)
			}
			wait <- false
		}()

		jsonChan, err := util.StreamRawJsonFromFile(NdJsonFile, workerCount)
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

		log.WithFields(log.Fields{"graph": GRAPH}).Info("loading data")
		return nil
	},
}
