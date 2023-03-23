package mongoload

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	//"io"
	//"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/util"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoHost = "mongodb://localhost"
var database = "gripdb"

var graph string
var vertexFile string
var edgeFile string
var dirPath string

var bulkBufferSize = 1000
var workerCount = 1

var logRate = 10000

var createGraph = false

func vertexSerialize(vertChan chan *gripql.Vertex, workers int) chan []byte {
	dataChan := make(chan []byte, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for v := range vertChan {
				doc := mongo.PackVertex(gdbi.NewElementFromVertex(v))
				rawBytes, err := bson.Marshal(doc)
				if err == nil {
					dataChan <- rawBytes
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(dataChan)
	}()
	return dataChan
}

func edgeSerialize(edgeChan chan *gripql.Edge, workers int) chan []byte {
	dataChan := make(chan []byte, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for e := range edgeChan {
				doc := mongo.PackEdge(gdbi.NewElementFromEdge(e))
				rawBytes, err := bson.Marshal(doc)
				if err == nil {
					dataChan <- rawBytes
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(dataChan)
	}()
	return dataChan
}

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "mongoload <graph>",
	Short: "Directly load data into mongodb",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" && dirPath == "" {
			return fmt.Errorf("no edge or vertex files were provided")
		}

		graph = args[0]

		// Connect to mongo and start the bulk load process
		log.Infof("Loading data into graph: %s", graph)
		client, err := mgo.NewClient(options.Client().ApplyURI(mongoHost))
		if err != nil {
			return err
		}
		err = client.Connect(context.TODO())
		if err != nil {
			return err
		}

		if createGraph {
			err = mongo.AddMongoGraph(client, database, graph)
			if err != nil {
				return err
			}
		}

		vertexCol := client.Database(database).Collection(fmt.Sprintf("%s_vertices", graph))
		edgeCol := client.Database(database).Collection(fmt.Sprintf("%s_edges", graph))

		if vertexFile != "" {
			log.Infof("Loading vertex file: %s", vertexFile)
			vertInserter := db.NewUnorderedBufferedBulkInserter(vertexCol, bulkBufferSize).
				SetBypassDocumentValidation(true).
				SetOrdered(false).
				SetUpsert(true)
			vertChan, err := util.StreamVerticesFromFile(vertexFile, workerCount)
			if err != nil {
				return err
			}
			dataChan := vertexSerialize(vertChan, workerCount)
			count := 0
			for d := range dataChan {
				vertInserter.InsertRaw(d)
				if count%logRate == 0 {
					log.Infof("Loaded %d vertices", count)
				}
				count++
			}
			vertInserter.Flush()
		}

		if edgeFile != "" {
			log.Infof("Loading edge file: %s", edgeFile)
			edgeInserter := db.NewUnorderedBufferedBulkInserter(edgeCol, bulkBufferSize).
				SetBypassDocumentValidation(true).
				SetOrdered(false).
				SetUpsert(true)
			edgeChan, err := util.StreamEdgesFromFile(edgeFile, workerCount)
			if err != nil {
				return err
			}
			dataChan := edgeSerialize(edgeChan, workerCount)
			count := 0
			for d := range dataChan {
				edgeInserter.InsertRaw(d)
				if count%logRate == 0 {
					log.Infof("Loaded %d edges", count)
				}
				count++
			}
			edgeInserter.Flush()
		}

		if dirPath != "" {
			if glob, err := filepath.Glob(filepath.Join(dirPath, "*.vertex.json.gz")); err == nil {
				vertexCount := 0
				vertInserter := db.NewUnorderedBufferedBulkInserter(vertexCol, bulkBufferSize).
					SetBypassDocumentValidation(true).
					SetOrdered(false).
					SetUpsert(true)
				for _, vertexFile := range glob {
					log.Infof("Loading vertex file: %s", vertexFile)
					vertChan, err := util.StreamVerticesFromFile(vertexFile, workerCount)
					if err != nil {
						return err
					}
					dataChan := vertexSerialize(vertChan, workerCount)
					for d := range dataChan {
						vertInserter.InsertRaw(d)
						if vertexCount%logRate == 0 {
							log.Infof("Loaded %d vertices", vertexCount)
						}
						vertexCount++
					}
				}
				vertInserter.Flush()
			}

			if glob, err := filepath.Glob(filepath.Join(dirPath, "*.edge.json.gz")); err == nil {
				edgeCount := 0
				edgeInserter := db.NewUnorderedBufferedBulkInserter(edgeCol, bulkBufferSize).
					SetBypassDocumentValidation(true).
					SetOrdered(false).
					SetUpsert(true)
				for _, edgeFile := range glob {
					log.Infof("Loading edge file: %s", edgeFile)
					edgeChan, err := util.StreamEdgesFromFile(edgeFile, workerCount)
					if err != nil {
						return err
					}
					dataChan := edgeSerialize(edgeChan, workerCount)
					for d := range dataChan {
						edgeInserter.InsertRaw(d)
						if edgeCount%logRate == 0 {
							log.Infof("Loaded %d edges", edgeCount)
						}
						edgeCount++
					}
				}
				edgeInserter.Flush()
			}
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&mongoHost, "mongo-host", mongoHost, "mongo server url")
	flags.StringVar(&database, "database", database, "database name in mongo to store graph")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
	flags.StringVarP(&dirPath, "dir", "d", "", "dir file")
	flags.BoolVarP(&createGraph, "create", "c", false, "create graph")
	flags.IntVarP(&workerCount, "workers", "n", workerCount, "number of processing threads")
	flags.IntVar(&bulkBufferSize, "batch-size", bulkBufferSize, "mongo bulk load batch size")
}
