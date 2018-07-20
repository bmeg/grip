package mongoload

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/mongo"
	"github.com/bmeg/arachne/util"
	"github.com/bmeg/arachne/util/rpc"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/cobra"
)

var host = "localhost"
var database = "arachnedb"
var graph string
var vertexFile string
var edgeFile string

var batchSize = 15
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

		conn, err := aql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}

		graphs, err := conn.ListGraphs()
		if err != nil {
			return err
		}

		found := false
		for g := range graphs {
			if graph == g {
				found = true
			}
		}
		if !found {
			log.Println("Creating  graph:", graph)
			err := conn.AddGraph(graph)
			if err != nil {
				return err
			}
		}

		log.Println("Loading data into graph:", graph)

		session, err := mgo.Dial(host)
		if err != nil {
			return err
		}

		vertexCo := session.DB(database).C(fmt.Sprintf("%s_vertices", graph))
		edgeCo := session.DB(database).C(fmt.Sprintf("%s_edges", graph))

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
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
						log.Printf("Loaded %d vertices", count)
					}
				}
				if len(docBatch) > 0 {
					docChan <- docBatch
				}
			}()

			for batch := range docChan {
				for i := 0; i < maxRetries; i++ {
					bulk := vertexCo.Bulk()
					for _, data := range batch {
						bulk.Upsert(bson.M{"_id": data["_id"]}, data)
					}
					_, err = bulk.Run()
					if err == nil || !isNetError(err) {
						i = maxRetries
					} else {
						log.Printf("Refreshing Connection")
						session.Refresh()
					}
				}
			}
			log.Printf("Loaded %d vertices", count)
		}

		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
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
						log.Printf("Loaded %d edges", count)
					}
				}
				if len(docBatch) > 0 {
					docChan <- docBatch
				}
			}()

			for batch := range docChan {
				for i := 0; i < maxRetries; i++ {
					bulk := edgeCo.Bulk()
					for _, data := range batch {
						bulk.Upsert(bson.M{"_id": data["_id"]}, data)
					}
					_, err = bulk.Run()
					if err == nil || !isNetError(err) {
						i = maxRetries
					} else {
						log.Printf("Refreshing Connection")
						session.Refresh()
					}
				}
			}
			log.Printf("Loaded %d edges", count)
		}
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "mongo server url")
	flags.StringVar(&database, "database", database, "database name in mongo to store graph")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
}
