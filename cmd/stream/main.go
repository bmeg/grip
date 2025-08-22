package stream

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

var host = "localhost:8202"
var KafkaUsername = ""
var KafkaPassword = ""
var Topic = ""

// Fetch all requests from Kafka and recreate all write
// Ex command using ci env: grip stream localhost:9092 --KafkaUsername admin --KafkaPassword adminpassword --Topic gripHistory
var Cmd = &cobra.Command{
	Use:   "stream",
	Short: "Stream data into a graph from Kafka",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {

		kafkaHost := args[0]
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			return err
		}
		defer conn.Close()

		config := sarama.NewConfig()
		config.Version = sarama.V2_8_0_0
		config.Net.SASL.Enable = true
		config.Net.SASL.User = KafkaUsername
		config.Net.SASL.Password = KafkaPassword
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Producer.Return.Successes = true
		config.Net.SASL.Handshake = true
		config.Net.TLS.Enable = false
		if KafkaUsername == "" || KafkaPassword == "" || Topic == "" {
			return fmt.Errorf("Kafka username and password and topic must be populated")
		}

		consumer, err := sarama.NewConsumer([]string{kafkaHost}, config)
		if err != nil {
			return fmt.Errorf("failed to create Kafka consumer: %w", err)
		}
		defer consumer.Close()

		writeConsumer, err := consumer.ConsumePartition(Topic, 0, sarama.OffsetOldest)
		if err != nil {
			return fmt.Errorf("failed to consume partition: %w", err)
		}
		defer writeConsumer.Close()
		done := make(chan bool)

		go func() {
			defer close(done)
			for {
				// timeout after 3 seconds have elapsed and no new message has been sent to Consumer
				timeout := time.NewTimer(3 * time.Second)
				select {
				case msg, ok := <-writeConsumer.Messages():
					if !ok {
						log.Info("writeConsumer channel closed")
						return
					}
					timeout.Stop()
					Method := ""
					Path := ""
					for _, HeaderMsg := range msg.Headers {
						if bytes.Equal(HeaderMsg.Key, []byte("PATH")) {
							Path = string(HeaderMsg.Value)
						} else if bytes.Equal(HeaderMsg.Key, []byte("METHOD")) {
							Method = string(HeaderMsg.Value)
						}
					}
					if Path == "" || Method == "" {
						continue
					}

					fmt.Println("PATH: ", Path, "METHOD: ", Method)
					bulk := regexp.MustCompile("^/v1/graph$")
					graph := regexp.MustCompile("^/v1/graph/([^/]+)$")
					switch Method {
					case "DELETE":
						deleteVertex := regexp.MustCompile("^/v1/graph/([^/]+)/vertex/([^/]+)$")
						deleteEdge := regexp.MustCompile("^/v1/graph/([^/]+)/edge/([^/]+)$")
						switch {
						case bulk.MatchString(Path):
							g := gripql.DeleteData{}
							err := protojson.Unmarshal(msg.Value, &g)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "value": string(msg.Value)}).Error("consumer: bulk delete unmarshal vertex error")
								continue
							}
							err = conn.BulkDelete(&g)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "path": Path}).Errorf("consumer: bulk delete")
								continue
							}
						case deleteVertex.MatchString(Path):
							id := strings.Split(Path, "/")
							err := conn.DeleteVertex(id[len(id)-3], id[len(id)-1])
							if err != nil {
								log.WithFields(log.Fields{"error": err, "path": Path}).Errorf("consumer: delete vertex")
								continue
							}
						case deleteEdge.MatchString(Path):
							id := strings.Split(Path, "/")
							err := conn.DeleteEdge(id[len(id)-3], id[len(id)-1])
							if err != nil {
								log.WithFields(log.Fields{"error": err, "path": Path}).Errorf("consumer: delete edge")
								continue
							}
						case graph.MatchString(Path):
							paths := strings.Split(Path, "/")
							graphName := paths[len(paths)-1]
							err = conn.DeleteGraph(graphName)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "graph": graphName}).Errorf("consumer: delete graph")
							}
						default:
							continue
						}
					case "POST":
						addVertex := regexp.MustCompile("^/v1/graph/([^/]+)/vertex$")
						addEdge := regexp.MustCompile("^/v1/graph/([^/]+)/edge$")
						addSchema := regexp.MustCompile("^/v1/graph/([^/]+)/schema$")
						switch {
						case bulk.MatchString(Path):
							fmt.Printf("WE HERE: %s", string(msg.Value))
						case addVertex.MatchString(Path):
							v := gripql.Vertex{}
							err = protojson.Unmarshal(msg.Value, &v)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "value": string(msg.Value)}).Error("consumer: unmarshal vertex error")
								continue
							}
							id := strings.Split(Path, "/")
							err = conn.AddVertex(id[len(id)-2], &v)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "vertex": v}).Error("consumer: add vertex error")
								continue
							}
						case addEdge.MatchString(Path):
							e := gripql.Edge{}
							err = protojson.Unmarshal(msg.Value, &e)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "value": string(msg.Value)}).Error("consumer: unmarshal edge error")
								continue
							}
							id := strings.Split(Path, "/")
							err = conn.AddEdge(id[len(id)-2], &e)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "edge": e}).Error("consumer: add edge error")
								continue
							}
						case addSchema.MatchString(Path):
							graph := gripql.Graph{}
							err = protojson.Unmarshal(msg.Value, &graph)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "value": string(msg.Value)}).Error("consumer: unmarshal schema error")
								continue
							}
							err = conn.AddSchema(&graph)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "schema": graph}).Error("consumer: add schema error")
							}
						case graph.MatchString(Path):
							paths := strings.Split(Path, "/")
							graphName := paths[len(paths)-1]
							err = conn.AddGraph(graphName)
							if err != nil {
								log.WithFields(log.Fields{"error": err, "graph": graphName}).Error("consumer: add graph error")
							}
						default:
							continue
						}
					default:
						continue
					}
				case <-timeout.C:
					return
				}
			}
		}()

		<-done
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:8202", "grip server url")
	flags.StringVar(&KafkaUsername, "KafkaUsername", "", "Kafka Username")
	flags.StringVar(&KafkaPassword, "KafkaPassword", "", "Kafka Password")
	flags.StringVar(&Topic, "Topic", "gripHistory", "Kafka Topic")

}
