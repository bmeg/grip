package kvload

import (
	"fmt"
	"log"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/util"
	"github.com/spf13/cobra"
)

var dbPath = "grip.db"
var kvDriver = "badger"

var graph string
var vertexFile string
var edgeFile string

var batchSize = 1000

func found(set []string, val string) bool {
	for _, i := range set {
		if i == val {
			return true
		}
	}
	return false
}

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "kvload <graph>",
	Short: "Directly load data into key/value",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" {
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

		db.AddGraph(graph)
		kgraph, err := db.Graph(graph)
		if err != nil {
			return err
		}

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			count := 0
			vertexChan := make(chan []*gripql.Vertex, 100)
			vertexBatch := make([]*gripql.Vertex, 0, batchSize)
			go func() {
				for v := range util.StreamVerticesFromFile(vertexFile) {
					vertexBatch = append(vertexBatch, v)
					if len(vertexBatch) >= batchSize {
						vertexChan <- vertexBatch
						vertexBatch = make([]*gripql.Vertex, 0, batchSize)
					}
					count++
					if count%10000 == 0 {
						log.Printf("Loaded %d vertices", count)
					}
				}
				if len(vertexBatch) > 0 {
					vertexChan <- vertexBatch
				}
				log.Printf("Loaded %d vertices", count)
				close(vertexChan)
			}()

			for batch := range vertexChan {
				//serialize and store vertex
				if err := kgraph.AddVertex(batch); err != nil {
					log.Printf("%s", err)
				}
			}
		}

		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			count := 0
			edgeChan := make(chan []*gripql.Edge, 100)
			edgeBatch := make([]*gripql.Edge, 0, batchSize)
			go func() {
				for e := range util.StreamEdgesFromFile(edgeFile) {
					edgeBatch = append(edgeBatch, e)
					if len(edgeBatch) >= batchSize {
						edgeChan <- edgeBatch
						edgeBatch = make([]*gripql.Edge, 0, batchSize)
					}
					count++
					if count%10000 == 0 {
						log.Printf("Loaded %d edges", count)
					}
				}
				if len(edgeBatch) > 0 {
					edgeChan <- edgeBatch
				}
				log.Printf("Loaded %d edges", count)
				close(edgeChan)
			}()
			for batch := range edgeChan {
				//serialize and store vertex
				if err := kgraph.AddEdge(batch); err != nil {
					log.Printf("%s", err)
				}
			}
		}

		db.Close()
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&dbPath, "db", dbPath, "DB Path")
	flags.StringVar(&kvDriver, "driver", kvDriver, "KV Driver")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
	flags.IntVar(&batchSize, "batch-size", batchSize, "mongo bulk load batch size")
}
