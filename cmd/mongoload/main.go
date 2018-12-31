package mongoload

import (
	"fmt"
	"io"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
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
			count := 0

			docChan := make(chan []map[string]interface{}, 100)
			docBatch := make([]map[string]interface{}, 0, batchSize)
			go func() {
				defer close(docChan)
				for v := range util.StreamVerticesFromFile(vertexFile) {
					data := mongo.PackVertex(v)
					docBatch = append(docBatch, data)
					if len(docBatch) > batchSize {
						docChan <- docBatch
						docBatch = make([]map[string]interface{}, 0, batchSize)
					}
					count++
					if count%1000 == 0 {
						log.Infof("Loaded %d vertices", count)
					}
				}
				if len(docBatch) > 0 {
					docChan <- docBatch
				}
			}()

			for batch := range docChan {
				for i := 0; i < maxRetries; i++ {
					bulk := vertexCo.Bulk()
					bulk.Unordered()
					for _, data := range batch {
						bulk.Upsert(bson.M{"_id": data["_id"]}, data)
					}
					_, err = bulk.Run()
					if err == nil || !isNetError(err) {
						i = maxRetries
					} else {
						log.Infof("Refreshing Connection")
						session.Refresh()
					}
				}
			}
			log.Infof("Loaded %d vertices", count)
		}

		if edgeFile != "" {
			log.Infof("Loading edge file: %s", edgeFile)
			count := 0

			docChan := make(chan []map[string]interface{}, 100)
			docBatch := make([]map[string]interface{}, 0, batchSize)
			go func() {
				defer close(docChan)
				for e := range util.StreamEdgesFromFile(edgeFile) {
					data := mongo.PackEdge(e)
					if data["_id"] == "" {
						data["_id"] = bson.NewObjectId().Hex()
					}
					docBatch = append(docBatch, data)
					if len(docBatch) > batchSize {
						docChan <- docBatch
						docBatch = make([]map[string]interface{}, 0, batchSize)
					}
					count++
					if count%1000 == 0 {
						log.Infof("Loaded %d edges", count)
					}
				}
				if len(docBatch) > 0 {
					docChan <- docBatch
				}
			}()

			for batch := range docChan {
				for i := 0; i < maxRetries; i++ {
					bulk := edgeCo.Bulk()
					bulk.Unordered()
					for _, data := range batch {
						bulk.Upsert(bson.M{"_id": data["_id"]}, data)
					}
					_, err = bulk.Run()
					if err == nil || !isNetError(err) {
						i = maxRetries
					} else {
						log.Infof("Refreshing Connection")
						session.Refresh()
					}
				}
			}
			log.Infof("Loaded %d edges", count)
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
