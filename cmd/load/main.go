package load

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/golib"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"log"
	"strings"
)

var host string = "localhost:9090"
var graph string = "data"
var vertexFile string = ""
var edgeFile string = ""
var bundleFile string = ""

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
				return err
			}
			count := 0
			for line := range reader {
				v := aql.Vertex{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &v)
				conn.AddVertex(graph, v)
				count += 1
				if count%1000 == 0 {
					log.Printf("Loaded %d vertices", count)
				}
			}
		}
		if edgeFile != "" {
			log.Printf("Loading %s", edgeFile)
			reader, err := golib.ReadFileLines(edgeFile)
			if err != nil {
				return err
			}
			count := 0
			for line := range reader {
				e := aql.Edge{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &e)
				conn.AddEdge(graph, e)
				count += 1
				if count%1000 == 0 {
					log.Printf("Loaded %d edges", count)
				}
			}
		}

		if bundleFile != "" {
			log.Printf("Loading %s", bundleFile)
			reader, err := golib.ReadFileLines(bundleFile)
			if err != nil {
				return err
			}
			count := 0
			for line := range reader {
				e := aql.Bundle{}
				jsonpb.Unmarshal(strings.NewReader(string(line)), &e)
				conn.AddBundle(graph, e)
				count += 1
				if count%1000 == 0 {
					log.Printf("Loaded %d bundles", count)
				}
			}
		}

		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
	flags.StringVar(&graph, "graph", "data", "Graph")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
	flags.StringVar(&bundleFile, "bundle", "", "Edge Bundle File")
}
