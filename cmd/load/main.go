package load

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/util"
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
	Use:   "load <graph>",
	Short: "Load Data into Arachne Server",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if vertexFile == "" && edgeFile == "" && jsonFile == "" && yamlFile == "" {
			return fmt.Errorf("no inputs files were provided")
		}

		graph = args[0]
		log.Println("Loading data into graph:", graph)

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

		elemChan := make(chan *aql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %v", err)
			}
			wait <- false
		}()

		if vertexFile != "" {
			log.Printf("Loading %s", vertexFile)
			verts, errs := util.StreamVerticesFromFile(vertexFile)
			go func(verts chan *aql.Vertex) {
				count := 0
				for v := range verts {
					count++
					if count%1000 == 0 {
						log.Printf("Loaded %d vertices", count)
					}
					elemChan <- &aql.GraphElement{Graph: graph, Vertex: v}
				}
				log.Printf("Loaded %d vertices", count)
			}(verts)
			go func(errs chan error) {
				for e := range errs {
					log.Printf("Error loading vertices: %v", e)
				}
			}(errs)
		}

		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			edges, errs := util.StreamEdgesFromFile(edgeFile)
			go func(edges chan *aql.Edge) {
				count := 0
				for e := range edges {
					count++
					if count%1000 == 0 {
						log.Printf("Loaded %d edges", count)
					}
					elemChan <- &aql.GraphElement{Graph: graph, Edge: e}
				}
				log.Printf("Loaded %d edges", count)
			}(edges)
			go func(errs chan error) {
				for e := range errs {
					log.Printf("Error loading vertices: %v", e)
				}
			}(errs)
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		if jsonFile != "" {
			log.Printf("Loading %s", jsonFile)
			content, err := ioutil.ReadFile(jsonFile)
			if err != nil {
				return err
			}
			g := &aql.Graph{}
			if err := m.Unmarshal(bytes.NewReader(content), g); err != nil {
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
			if err := m.Unmarshal(bytes.NewReader(content), g); err != nil {
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
	flags.StringVar(&host, "host", host, "Arachne host server")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
	flags.StringVar(&jsonFile, "json", "", "JSON Graph File")
	flags.StringVar(&yamlFile, "yaml", "", "YAML Graph File")
}
