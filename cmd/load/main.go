package load

import (
	"fmt"

	"github.com/bmeg/grip/cmd/load/example"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/rpc"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var graph string
var vertexFile string
var edgeFile string
var jsonFile string
var yamlFile string

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

		resp, err := conn.ListGraphs()
		if err != nil {
			return err
		}

		found := false
		for _, g := range resp.Graphs {
			if graph == g {
				found = true
			}
		}
		if !found {
			log.WithFields(log.Fields{"graph": graph}).Info("creating graph")
			err := conn.AddGraph(graph)
			if err != nil {
				return err
			}
		}

		log.WithFields(log.Fields{"graph": graph}).Info("loading data")

		elemChan := make(chan *gripql.GraphElement)
		wait := make(chan bool)
		go func() {
			if err := conn.BulkAdd(elemChan); err != nil {
				log.Printf("bulk add error: %v", err)
			}
			wait <- false
		}()

		if vertexFile != "" {
			log.Infof("Loading vertex file: %s", vertexFile)
			count := 0
			for v := range util.StreamVerticesFromFile(vertexFile) {
				count++
				if count%1000 == 0 {
					log.Infof("Loaded %d vertices", count)
				}
				elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
			}
			log.Infof("Loaded %d vertices", count)
		}

		if edgeFile != "" {
			log.Infof("Loading edge file: %s", edgeFile)
			count := 0
			for e := range util.StreamEdgesFromFile(edgeFile) {
				count++
				if count%1000 == 0 {
					log.Infof("Loaded %d edges", count)
				}
				elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
			}
			log.Infof("Loaded %d edges", count)
		}

		if jsonFile != "" {
			log.Infof("Loading json file: %s", jsonFile)
			graphs, err := gripql.ParseJSONGraphFile(yamlFile)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				for _, v := range g.Vertices {
					elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
				}
				log.Infof("Loaded %d vertices", len(g.Vertices))
				for _, e := range g.Edges {
					elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
				}
				log.Infof("Loaded %d edges", len(g.Edges))
			}
		}

		if yamlFile != "" {
			log.Infof("Loading YAML file: %s", yamlFile)
			graphs, err := gripql.ParseYAMLGraphFile(yamlFile)
			if err != nil {
				return err
			}
			for _, g := range graphs {
				for _, v := range g.Vertices {
					elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
				}
				log.Infof("Loaded %d vertices", len(g.Vertices))
				for _, e := range g.Edges {
					elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
				}
				log.Infof("Loaded %d edges", len(g.Edges))
			}
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
