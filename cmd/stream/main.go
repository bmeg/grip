package stream

import (
	"github.com/IBM/sarama"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var kafka = "localhost:9092"
var host = "localhost:8202"
var graph string
var vertexTopic = "grip_vertex"
var edgeTopic = "grip_edge"

// Cmd is the base command called by the cobra command line system
var Cmd = &cobra.Command{
	Use:   "stream <graph>",
	Short: "Stream data into a graph from Kafka",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		graph = args[0]
		log.WithFields(log.Fields{"kafka": kafka, "graph": graph}).Errorf("Streaming data from Kafka into graph")

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
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
				v := gripql.Vertex{}
				err := protojson.Unmarshal(msg.Value, &v)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("vertex consumer: unmarshal error")
					continue
				}
				err = conn.AddVertex(graph, &v)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("vertex consumer: add error")
					continue
				}
				count++
				if count%1000 == 0 {
					log.Infof("Loaded %d vertices", count)
				}
			}
			done <- true
		}()

		go func() {
			count := 0
			for msg := range edgeConsumer.Messages() {
				e := gripql.Edge{}
				err := protojson.Unmarshal(msg.Value, &e)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("edge consumer: unmarshal error")
					continue
				}
				err = conn.AddEdge(graph, &e)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("edge consumer: add error")
					continue
				}
				count++
				if count%1000 == 0 {
					log.Infof("Loaded %d edges", count)
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
	flags.StringVar(&host, "host", "localhost:8202", "grip server url")
	flags.StringVar(&vertexTopic, "vertex", "grip_vertex", "vertex topic name")
	flags.StringVar(&edgeTopic, "edge", "grip_edge", "edge topic name")
}
