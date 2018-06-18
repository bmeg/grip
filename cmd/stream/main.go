package stream

import (
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
)

var kafka = "localhost:9092"
var host = "localhost:8202"
var graph string
var vertexTopic = "arachne_vertex"
var edgeTopic = "arachne_edge"

// Cmd is the base command called by the cobra command line system
var Cmd = &cobra.Command{
	Use:   "stream <graph>",
	Short: "Stream data into a graph from Kafka",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph = args[0]
		log.Printf("Streaming data from Kafka instance %s into graph %s", kafka, graph)

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

		done := make(chan bool)

		go func() {
			count := 0
			for msg := range vertexConsumer.Messages() {
				v := aql.Vertex{}
				err := jsonpb.Unmarshal(strings.NewReader(string(msg.Value)), &v)
				if err != nil {
					log.Println("vertex consumer: unmarshal error", err)
					continue
				}
				err = conn.AddVertex(graph, &v)
				if err != nil {
					log.Println("vertex consumer: add error", err)
					continue
				}
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
				err := jsonpb.Unmarshal(strings.NewReader(string(msg.Value)), &e)
				if err != nil {
					log.Println("edge consumer: unmarshal error", err)
					continue
				}
				err = conn.AddEdge(graph, &e)
				if err != nil {
					log.Println("edge consumer: add error", err)
					continue
				}
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
	flags.StringVar(&kafka, "kafka", "localhost:9092", "Kafka server url")
	flags.StringVar(&host, "host", "localhost:8202", "arachne server url")
	flags.StringVar(&vertexTopic, "vertex", "arachne_vertex", "vertex topic name")
	flags.StringVar(&edgeTopic, "edge", "arachne_vertex", "edge topic name")
}
