package kvload

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
	"github.com/paulbellamy/ratecounter"
	"github.com/spf13/cobra"
)

var dbPath = "grip.db"
var kvDriver = "badger"
var graph string
var vertexFile string
var edgeFile string
var vertexManifestFile string
var edgeManifestFile string

var workerCount = 1

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "kvload <graph>",
	Short: "Directly load data into key/value",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" && vertexManifestFile == "" && edgeManifestFile == "" {
			return fmt.Errorf("no edge or vertex files were provided")
		}

		graph = args[0]

		log.GetLogger().SetLevel(log.DebugLevel)

		// Create the graph  if it doesn't already exist.
		// Creating the graph also results in the creation of indices
		// for the edge/vertex collections.
		kv, err := kvi.NewKVInterface(kvDriver, dbPath, &kvi.Options{BulkLoad: true})
		if err != nil {
			return err
		}
		db := kvgraph.NewKVGraph(kv)
		defer db.Close()

		err = db.AddGraph(graph)
		if err != nil {
			if strings.Contains(err.Error(), "invalid graph name") {
				return err
			}
		}
		kgraph, err := db.Graph(graph)
		if err != nil {
			return err
		}

		vertexFileArray := []string{}
		edgeFileArray := []string{}

		if vertexManifestFile != "" {
			reader, err := util.StreamLines(vertexManifestFile, 10)
			if err != nil {
				return err
			}
			for line := range reader {
				if line != "" {
					vertexFileArray = append(vertexFileArray, string(line))
				}
			}
		}

		if edgeManifestFile != "" {
			reader, err := util.StreamLines(edgeManifestFile, 10)
			if err != nil {
				return err
			}
			for line := range reader {
				if line != "" {
					edgeFileArray = append(edgeFileArray, string(line))
				}
			}
		}

		if vertexFile != "" {
			vertexFileArray = append(vertexFileArray, vertexFile)
		}
		if edgeFile != "" {
			edgeFileArray = append(edgeFileArray, edgeFile)
		}

		graphChan := make(chan *gdbi.GraphElement, 10)
		wg := &sync.WaitGroup{}
		go func() {
			wg.Add(1)
			if err := kgraph.BulkAdd(graphChan); err != nil {
				log.Errorf("BulkdAdd: %v", err)
			}
			wg.Done()
		}()

		vertexCounter := ratecounter.NewRateCounter(10 * time.Second)
		for _, vertexFile := range vertexFileArray {
			log.Infof("Loading %s", vertexFile)
			count := 0
			vertChan, err := util.StreamVerticesFromFile(vertexFile, workerCount)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Errorf("Error reading file: %s", vertexFile)
				continue
			}
			for v := range vertChan {
				graphChan <- &gdbi.GraphElement{Graph: graph, Vertex: gdbi.NewElementFromVertex(v)}
				count++
				vertexCounter.Incr(1)
				if count%10000 == 0 {
					log.Infof("Loaded %d vertices (%d/sec)", count, vertexCounter.Rate()/10)
				}
			}
			log.Infof("Loaded %d vertices (%d/sec)", count, vertexCounter.Rate()/10)
		}

		edgeCounter := ratecounter.NewRateCounter(10 * time.Second)
		for _, edgeFile := range edgeFileArray {
			log.Infof("Loading %s", edgeFile)
			count := 0
			edgeChan, err := util.StreamEdgesFromFile(edgeFile, workerCount)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Errorf("Error reading file: %s", edgeFile)
				continue
			}
			for e := range edgeChan {
				graphChan <- &gdbi.GraphElement{Graph: graph, Edge: gdbi.NewElementFromEdge(e)}
				count++
				edgeCounter.Incr(1)
				if count%10000 == 0 {
					log.Infof("Loaded %d edges (%d/sec)", count, edgeCounter.Rate()/10)
				}
			}
			log.Infof("Loaded %d edges (%d/sec)", count, edgeCounter.Rate()/10)
		}

		close(graphChan)
		wg.Wait()
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&dbPath, "db", dbPath, "DB Path")
	flags.StringVar(&kvDriver, "driver", kvDriver, "KV Driver")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
	flags.StringVar(&vertexManifestFile, "vertex-manifest", "", "vertex manifest file")
	flags.IntVarP(&workerCount, "workers", "n", workerCount, "number of processing threads")
	flags.StringVar(&edgeManifestFile, "edge-manifest", "", "edge manifest file")
}
