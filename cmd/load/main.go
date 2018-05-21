package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
var graph string
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

		if graph == "" {
			return fmt.Errorf("must specify which graph to load data into")
		}

		conn, err := aql.Connect(host, true)
		if err != nil {
			return err
		}

		graphs := conn.GetGraphList()
		if !found(graphs, graph) {
			err := conn.AddGraph(graph)
			if err != nil {
				return err
			}
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		elemChan := make(chan *aql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %s", err)
			}
			wait <- false
		}()

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			reader, err := golib.ReadFileLines(vertexFile)
			if err != nil {
				return err
			}
			count := 0
			for line := range reader {
				v := aql.Vertex{}
				err := m.Unmarshal(strings.NewReader(string(line)), &v)
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to unmarshal vertex: %v", err)
				}
				elemChan <- &aql.GraphElement{Graph: graph, Vertex: &v}
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d vertices", count)
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
			for line := range reader {
				e := aql.Edge{}
				err := m.Unmarshal(strings.NewReader(string(line)), &e)
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to unmarshal vertex: %v", err)
				}
				elemChan <- &aql.GraphElement{Graph: graph, Edge: &e}
				count++
				if count%1000 == 0 {
					log.Printf("Loaded %d edges", count)
				}
			}
			log.Printf("Loaded %d edges", count)
		}

		if jsonFile != "" {
			log.Printf("Loading %s", jsonFile)
			content, err := ioutil.ReadFile(jsonFile)
			if err != nil {
				return err
			}
			g := &aql.Graph{}
			if err := jsonpb.Unmarshal(bytes.NewReader(content), g); err != nil {
				return err
			}
			for _, v := range g.Vertices {
				elemChan <- &aql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Printf("Loaded %d vertices", len(g.Vertices))
			for _, e := range g.Edges {
				elemChan <- &aql.GraphElement{Graph: graph, Edge: e}
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
			g := &aql.Graph{}
			if err := jsonpb.Unmarshal(bytes.NewReader(content), g); err != nil {
				return err
			}
			for _, v := range g.Vertices {
				elemChan <- &aql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Printf("Loaded %d vertices", len(g.Vertices))
			for _, e := range g.Edges {
				elemChan <- &aql.GraphElement{Graph: graph, Edge: e}
			}
			log.Printf("Loaded %d edges", len(g.Edges))
		}

		close(elemChan)
		<-wait

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "", "Graph")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
	flags.StringVar(&jsonFile, "json", "", "JSON Graph File")
	flags.StringVar(&yamlFile, "yaml", "", "YAML Graph File")
}
