package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/cmd/load/example"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var host = "localhost:8202"
var graph string
var vertexFile string
var edgeFile string
var jsonFile string
var yamlFile string

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
	Use:   "load <graph>",
	Short: "Load data into a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" && jsonFile == "" && yamlFile == "" {
			return fmt.Errorf("no input files were provided")
		}

		graph = args[0]

		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
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

		elemChan := make(chan *gripql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %v", err)
			}
			wait <- false
		}()

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			count := 0
			for v := range util.StreamVerticesFromFile(vertexFile) {
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d vertices", count)
				}
				elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Printf("Loaded %d vertices", count)

		}

		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			count := 0
			for e := range util.StreamEdgesFromFile(edgeFile) {
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d edges", count)
				}
				elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
			}
			log.Printf("Loaded %d edges", count)
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		if jsonFile != "" {
			log.Printf("Loading %s", jsonFile)
			content, err := ioutil.ReadFile(jsonFile)
			if err != nil {
				return err
			}
			g := &gripql.Graph{}
			if err := m.Unmarshal(bytes.NewReader(content), g); err != nil {
				return err
			}
			for _, v := range g.Vertices {
				elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Printf("Loaded %d vertices", len(g.Vertices))
			for _, e := range g.Edges {
				elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
			}
			log.Printf("Loaded %d edges", len(g.Edges))
		}

		if yamlFile != "" {
			log.Printf("Loading %s", yamlFile)
			yamlContent, err := ioutil.ReadFile(yamlFile)
			if err != nil {
				return err
			}
			t := map[string]interface{}{}
			err = yaml.Unmarshal([]byte(yamlContent), &t)
			if err != nil {
				return err
			}
			content, err := json.Marshal(mapNormalize(t))
			if err != nil {
				return err
			}
			g := &gripql.Graph{}
			if err := m.Unmarshal(bytes.NewReader(content), g); err != nil {
				return err
			}
			for _, v := range g.Vertices {
				elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Printf("Loaded %d vertices", len(g.Vertices))
			for _, e := range g.Edges {
				elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
			}
			log.Printf("Loaded %d edges", len(g.Edges))
		}

		close(elemChan)
		<-wait

		return nil
	},
}

func init() {
	Cmd.AddCommand(example.Cmd)
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
	flags.StringVar(&vertexFile, "vertex", "", "vertex file")
	flags.StringVar(&edgeFile, "edge", "", "edge file")
	flags.StringVar(&jsonFile, "json", "", "JSON graph file")
	flags.StringVar(&yamlFile, "yaml", "", "YAML graph file")
}
