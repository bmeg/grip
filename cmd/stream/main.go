package stream

import (
	//"fmt"
	"github.com/bmeg/arachne/aql"
	//"github.com/bmeg/golib"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var kafka = "localhost:9092"
var host = "localhost:8202"
var graph = "data"
var vertexTopic = "arachne_vertex"
var edgeTopic = "arachne_edge"

// Cmd is the base command called by the cobra command line system
var Cmd = &cobra.Command{
	Use:   "stream",
	Short: "Stream Data into Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Printf("Streaming Data from %s", kafka)
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		consumer, err := sarama.NewConsumer([]string{kafka}, nil)
		if err != nil {
			panic(err)
		}

		vertexConsumer, err := consumer.ConsumePartition(vertexTopic, 0, sarama.OffsetOldest)
		edgeConsumer, err := consumer.ConsumePartition(edgeTopic, 0, sarama.OffsetOldest)
		//timer := time.AfterFunc(5 * time.Second, partitionConsumer.AsyncClose )

		done := make(chan bool)

		go func() {
			count := 0
			for msg := range vertexConsumer.Messages() {
				v := aql.Vertex{}
				jsonpb.Unmarshal(strings.NewReader(string(msg.Value)), &v)
				conn.AddVertex(graph, v)
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d vertices", count)
				}
			}
			done <- true
		}()

		go func() {
			count := 0
			for msg := range edgeConsumer.Messages() {
				e := aql.Edge{}
				jsonpb.Unmarshal(strings.NewReader(string(msg.Value)), &e)
				conn.AddEdge(graph, e)
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d edges", count)
				}
			}
			done <- true
		}()
		<-done
		<-done
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&kafka, "kafka", "localhost:9092", "Kafka Server")
	flags.StringVar(&host, "host", "localhost:8202", "Arachne Server")
	flags.StringVar(&graph, "graph", "data", "Graph")
	flags.StringVar(&vertexTopic, "vertex", "arachne_vertex", "Vertex File")
	flags.StringVar(&edgeTopic, "edge", "arachne_vertex", "Edge File")
}
