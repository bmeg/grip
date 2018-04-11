package load

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/golib"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var host = "localhost:8202"
var graph = "data"
var vertexFile string
var edgeFile string
var jsonFile string
var yamlFile string

func found(set []string, val string) bool {
	for _, i := range set {
		if i == val {
			return true
		}
	}
	return false
}

func mapNormalize(v interface{}) interface{} {
	if base, ok := v.(map[interface{}]interface{}); ok {
		out := map[string]interface{}{}
		for k, v := range base {
			out[k.(string)] = mapNormalize(v)
		}
		return out
	} else if base, ok := v.(map[string]interface{}); ok {
		out := map[string]interface{}{}
		for k, v := range base {
			out[k] = mapNormalize(v)
		}
		return out
	} else if base, ok := v.([]interface{}); ok {
		out := make([]interface{}, len(base))
		for i, v := range base {
			out[i] = mapNormalize(v)
		}
		return out
	}
	return v
}

// Cmd is the declaration of the command line
var Cmd = &cobra.Command{
	Use:   "load",
	Short: "Load Data into Arachne Server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Printf("Loading Data")
		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			reader, err := golib.ReadFileLines(vertexFile)
			if err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			count := 0
			elemChan := make(chan *aql.GraphElement)
			wait := make(chan bool)
			go func() {
				if err := conn.StreamElements(elemChan); err != nil {
					log.Printf("Load Error: %s", err)
				}
				wait <- false
			}()
			for line := range reader {
				v := aql.Vertex{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &v)
				elemChan <- &aql.GraphElement{Graph: graph, Vertex: &v}
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d vertices", count)
				}
			}
			log.Printf("Loaded %d vertices", count)
			close(elemChan)
			<-wait
		}
		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			reader, err := golib.ReadFileLines(edgeFile)
			if err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			count := 0
			elemChan := make(chan *aql.GraphElement)
			wait := make(chan bool)
			go func() {
				if err := conn.StreamElements(elemChan); err != nil {
					log.Printf("StreamError: %s", err)
				}
				wait <- false
			}()
			umarsh := jsonpb.Unmarshaler{AllowUnknownFields: true}
			for line := range reader {
				if len(line) > 0 {
					e := aql.Edge{}
					err := umarsh.Unmarshal(strings.NewReader(string(line)), &e)
					if err != nil {
						log.Printf("Error: %s : '%s'", err, line)
					} else {
						elemChan <- &aql.GraphElement{Graph: graph, Edge: &e}
						count++
					}
					if count%1000 == 0 {
						log.Printf("Loaded %d edges", count)
					}
				}
			}
			log.Printf("Loaded %d edges", count)
			close(elemChan)
			<-wait
		}

		if jsonFile != "" {
			log.Printf("Loading %s", jsonFile)
			graphs := conn.GetGraphList()
			if !found(graphs, graph) {
				conn.AddGraph(graph)
			}
			content, err := ioutil.ReadFile(jsonFile)
			if err != nil {
				log.Printf("Error reading file: %s", err)
				return err
			}
			e := aql.Graph{}
			if err := jsonpb.Unmarshal(strings.NewReader(string(content)), &e); err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			conn.AddSubGraph(graph, &e)
			log.Printf("Subgraph Loaded")
		}

		if yamlFile != "" {
			log.Printf("Loading %s", yamlFile)
			graphs := conn.GetGraphList()
			if !found(graphs, graph) {
				conn.AddGraph(graph)
			}
			yamlContent, err := ioutil.ReadFile(yamlFile)
			if err != nil {
				log.Printf("Error reading file: %s", err)
				return err
			}

			t := map[string]interface{}{}
			err = yaml.Unmarshal([]byte(yamlContent), &t)
			if err != nil {
				log.Fatalf("error: %v", err)
			}
			content, err := json.Marshal(mapNormalize(t))
			if err != nil {
				log.Fatalf("error: %v", err)
			}
			e := aql.Graph{}
			if err := jsonpb.Unmarshal(strings.NewReader(string(content)), &e); err != nil {
				log.Printf("Error: %s", err)
				return err
			}
			conn.AddSubGraph(graph, &e)
			log.Printf("Subgraph Loaded")
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "data", "Graph")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
	flags.StringVar(&jsonFile, "json", "", "JSON Graph File")
	flags.StringVar(&yamlFile, "yaml", "", "YAML Graph File")
}
