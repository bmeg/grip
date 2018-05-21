package mongoload

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/bmeg/golib"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/spf13/cobra"
)

var host = "localhost"
var database = "arachne"
var graph = "data"
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
	Use:   "mongoload",
	Short: "Direct Load Data into mongo Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Printf("Loading Data")

		session, err := mgo.Dial(host)
		if err != nil {
			fmt.Printf("Error %s", err)
			return err
		}

		vertexCo := session.DB(database).C(fmt.Sprintf("%s_vertices", graph))
		edgeCo := session.DB(database).C(fmt.Sprintf("%s_edges", graph))

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			reader, err := golib.ReadFileLines(vertexFile)
			if err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			count := 0

			docChan := make(chan []map[string]interface{}, 100)
			docBatch := make([]map[string]interface{}, 0, batchSize)
			go func() {
				defer close(docChan)
				for line := range reader {
					data := map[string]interface{}{}
					if err := json.Unmarshal(line, &data); err == nil {
						data["_id"] = data["gid"]
						delete(data, "gid")
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
			reader, err := golib.ReadFileLines(edgeFile)
			if err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			count := 0

			docChan := make(chan []map[string]interface{}, 100)
			docBatch := make([]map[string]interface{}, 0, batchSize)
			go func() {
				defer close(docChan)
				for line := range reader {
					data := map[string]interface{}{}
					json.Unmarshal(line, &data)
					if x, ok := data["gid"]; ok {
						data["_id"] = x
						delete(data, "gid")
					} else {
						data["_id"] = bson.NewObjectId().Hex()
					}
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
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&database, "database", database, "Host Server")
	flags.StringVar(&graph, "graph", graph, "Graph")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
}
