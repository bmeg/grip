package load

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/golib"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"log"
	"strings"
)

var host = "localhost:8202"
var graph = "data"
var vertexFile string
var edgeFile string
var bundleFile string

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
				return err
			}
			count := 0
			elemChan := make(chan aql.GraphElement)
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
				//conn.AddVertex(graph, v)
				elemChan <- aql.GraphElement{Graph: graph, Vertex: &v}
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
				return err
			}
			count := 0
			elemChan := make(chan aql.GraphElement)
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
						//conn.AddEdge(graph, e)
						elemChan <- aql.GraphElement{Graph: graph, Edge: &e}
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
				count++
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
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "data", "Graph")
	flags.StringVar(&vertexFile, "vertex", "", "Vertex File")
	flags.StringVar(&edgeFile, "edge", "", "Edge File")
	flags.StringVar(&bundleFile, "bundle", "", "Edge Bundle File")
}
