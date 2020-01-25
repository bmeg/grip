package mongoload

import (
	"fmt"
	"io"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/cobra"
)

var mongoHost = "localhost"
var host = "localhost:8202"
var database = "gripdb"

var graph string
var vertexFile string
var edgeFile string

var batchSize = 1000
var maxRetries = 3

func found(set []string, val string) bool {
	for _, i := range set {
		if i == val {
			return true
		}
	}
	return false
}

// MaxRetries is the number of times driver will reconnect on connection failure
// TODO, move to per instance config, rather then global
var MaxRetries = 3

func isNetError(e error) bool {
	if e == io.EOF {
		return true
	}
	if b, ok := e.(*mgo.BulkError); ok {
		for _, c := range b.Cases() {
			if c.Err == io.EOF {
				return true
			}
			if strings.Contains(c.Err.Error(), "connection") {
				return true
			}
		}
	}
	return false
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

		// Create the graph  if it doesn't already exist.
		// Creating the graph also results in the creation of indices
		// for the edge/vertex collections.
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
			log.Infof("Creating graph: %s", graph)
			err := conn.AddGraph(graph)
			if err != nil {
				return err
			}
		}

		// Connect to mongo and start the bulk load process
		log.Infof("Loading data into graph: %s", graph)
		session, err := mgo.Dial(mongoHost)
		if err != nil {
			return err
		}

		vertexCo := session.DB(database).C(fmt.Sprintf("%s_vertices", graph))
		edgeCo := session.DB(database).C(fmt.Sprintf("%s_edges", graph))

		if vertexFile != "" {
			log.Infof("Loading vertex file: %s", vertexFile)

			bulkVertChan := make(chan []map[string]interface{}, 5)
			docBatch := make([]map[string]interface{}, 0, batchSize)

			go func() {
				count := 0
				for batch := range bulkVertChan {
					for i := 0; i < maxRetries; i++ {
						bulk := vertexCo.Bulk()
						bulk.Unordered()
						for _, data := range batch {
							bulk.Upsert(bson.M{"_id": data["_id"]}, data)
							count++
						}
						_, err = bulk.Run()
						if err == nil || !isNetError(err) {
							i = maxRetries
						} else {
							log.Infof("Refreshing Connection")
							session.Refresh()
						}
					}
					if count%1000 == 0 {
						log.Infof("Loaded %d vertices", count)
					}
				}
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
					for i := 0; i < maxRetries; i++ {
						bulk := edgeCo.Bulk()
						bulk.Unordered()
						for _, data := range batch {
							bulk.Upsert(bson.M{"_id": data["_id"]}, data)
							count++
						}
						_, err = bulk.Run()
						if err == nil || !isNetError(err) {
							i = maxRetries
						} else {
							log.Infof("Refreshing Connection")
							session.Refresh()
						}
					}
					if count%1000 == 0 {
						log.Infof("Loaded %d edges", count)
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
				if data["_id"] == "" {
					data["_id"] = bson.NewObjectId().Hex()
				}
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
	flags.StringVar(&host, "grip-host", host, "grip rpc server address")
	flags.StringVar(&database, "database", database, "database name in mongo to store graph")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
	flags.IntVar(&batchSize, "batch-size", batchSize, "mongo bulk load batch size")
}
