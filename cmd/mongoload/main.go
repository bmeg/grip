package mongoload

import (
	"context"
	"fmt"
	"sync"

	//"io"
	//"strings"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/util"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoHost = "localhost"
var database = "gripdb"

var graph string
var vertexFile string
var edgeFile string

var batchSize = 1000

func docWriter(col *mgo.Collection, docChan chan bson.M, sn *sync.WaitGroup) {
	defer sn.Done()
	docBatch := make([]mgo.WriteModel, 0, batchSize)
	for ent := range docChan {
		i := mgo.NewInsertOneModel()
		i.SetDocument(ent)
		docBatch = append(docBatch, i)
		if len(docBatch) > batchSize {
			_, err := col.BulkWrite(context.Background(), docBatch)
			if err != nil {
				log.Errorf("%s", err)
			}
			docBatch = make([]mgo.WriteModel, 0, batchSize)
		}
	}
	if len(docBatch) > 0 {
		col.BulkWrite(context.Background(), docBatch)
	}
}

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "mongoload <graph>",
	Short: "Directly load data into mongodb",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" {
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

		mongo.AddMongoGraph(client, database, graph)

		vertexCol := client.Database(database).Collection(fmt.Sprintf("%s_vertices", graph))
		edgeCol := client.Database(database).Collection(fmt.Sprintf("%s_edges", graph))

		vertexDocChan := make(chan bson.M, 5)
		edgeDocChan := make(chan bson.M, 5)

		s := &sync.WaitGroup{}
		go docWriter(edgeCol, edgeDocChan, s)
		s.Add(1)
		go docWriter(vertexCol, vertexDocChan, s)
		s.Add(1)

		if vertexFile != "" {
			log.Infof("Loading vertex file: %s", vertexFile)

			bulkVertChan := make(chan []map[string]interface{}, 5)
			docBatch := make([]map[string]interface{}, 0, batchSize)

			go func() {
				count := 0
				for batch := range bulkVertChan {
					for _, data := range batch {
						vertexDocChan <- data
						if count%1000 == 0 {
							log.Infof("Loaded %d vertices", count)
						}
					}
				}
				close(vertexDocChan)
				log.Infof("Loaded %d vertices", count)
			}()

			vertChan, err := util.StreamVerticesFromFile(vertexFile)
			if err != nil {
				return err
			}
			for v := range vertChan {
				data := mongo.PackVertex(v)
				docBatch = append(docBatch, data)
				if len(docBatch) > batchSize {
					bulkVertChan <- docBatch
					docBatch = make([]map[string]interface{}, 0, batchSize)
				}
			}
			if len(docBatch) > 0 {
				bulkVertChan <- docBatch
			}
			close(bulkVertChan)
		}

		if edgeFile != "" {
			log.Infof("Loading edge file: %s", edgeFile)

			bulkEdgeChan := make(chan []map[string]interface{}, 5)
			docBatch := make([]map[string]interface{}, 0, batchSize)

			go func() {
				count := 0
				for batch := range bulkEdgeChan {
					for _, data := range batch {
						edgeDocChan <- data
						if count%1000 == 0 {
							log.Infof("Loaded %d edges", count)
						}
					}
				}
				log.Infof("Loaded %d edges", count)
			}()

			edgeChan, err := util.StreamEdgesFromFile(edgeFile)
			if err != nil {
				return err
			}
			for e := range edgeChan {
				data := mongo.PackEdge(e)
				//if data["_id"] == "" {
				//	data["_id"] = bson.NewObjectId().Hex()
				//}
				docBatch = append(docBatch, data)
				if len(docBatch) > batchSize {
					bulkEdgeChan <- docBatch
					docBatch = make([]map[string]interface{}, 0, batchSize)
				}
			}
			if len(docBatch) > 0 {
				bulkEdgeChan <- docBatch
			}
			close(bulkEdgeChan)
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
	flags.IntVar(&batchSize, "batch-size", batchSize, "mongo bulk load batch size")
}
