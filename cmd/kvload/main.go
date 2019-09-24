package kvload

import (
	"fmt"
	"sync"
	"time"

	"github.com/bmeg/golib"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/util"
	"github.com/paulbellamy/ratecounter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var dbPath = "grip.db"
var kvDriver = "badger"
var graph string
var vertexFile string
var edgeFile string
var vertexManifestFile string
var edgeManifestFile string

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

		// Create the graph  if it doesn't already exist.
		// Creating the graph also results in the creation of indices
		// for the edge/vertex collections.
		kv, err := kvgraph.NewKVInterface(kvDriver, dbPath, &kvi.Options{BulkLoad: true})
		if err != nil {
			return err
		}
		db := kvgraph.NewKVGraph(kv)
		defer db.Close()

		_ = db.AddGraph(graph)
		kgraph, err := db.Graph(graph)
		if err != nil {
			return err
		}

		vertexFileArray := []string{}
		edgeFileArray := []string{}

		if vertexManifestFile != "" {
			reader, err := golib.ReadFileLines(vertexManifestFile)
			if err == nil {
				for line := range reader {
					vertexFileArray = append(vertexFileArray, string(line))
				}
			}
		}
		if edgeManifestFile != "" {
			reader, err := golib.ReadFileLines(edgeManifestFile)
			if err == nil {
				for line := range reader {
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

		graphChan := make(chan *gripql.GraphElement, 1000)
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
			for v := range util.StreamVerticesFromFile(vertexFile) {
				graphChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
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
			log.Printf("Loading %s", edgeFile)
			count := 0
			for e := range util.StreamEdgesFromFile(edgeFile) {
				graphChan <- &gripql.GraphElement{Graph: graph, Edge: e}
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
	flags.StringVar(&edgeManifestFile, "edge-manifest", "", "edge manifest file")
}
