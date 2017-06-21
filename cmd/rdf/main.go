package rdf

import (
	"io"
	"log"
	"os"
	//"fmt"
	"compress/gzip"
	"github.com/bmeg/arachne/aql"
	"github.com/knakk/rdf"
	"github.com/spf13/cobra"
)

var host string = "localhost:9090"
var graph string = "default"

func LoadRDFCmd(cmd *cobra.Command, args []string) error {
	f, err := os.Open(args[0])
	if err != nil {
		log.Printf("Error: %s", err)
		os.Exit(1)
	}
	conn, err := aql.Connect(host, true)
	if err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

	vert_map := map[string]int{}

	count := 0
	fz, _ := gzip.NewReader(f)
	dec := rdf.NewTripleDecoder(fz, rdf.RDFXML)
	var cur_vertex *aql.Vertex = nil
	cur_subj := ""
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		subj := triple.Subj.String()
		if subj != cur_subj && cur_vertex != nil {
			conn.AddVertex(graph, *cur_vertex)
			cur_vertex = nil
		}
		cur_subj = subj
		if _, ok := vert_map[subj]; !ok {
			conn.AddVertex(graph, aql.Vertex{Gid: subj})
			vert_map[subj] = 1
		}
		if triple.Obj.Type() == rdf.TermLiteral {
			if cur_vertex == nil {
				cur_vertex = &aql.Vertex{Gid: subj}
			}
			cur_vertex.SetProperty(triple.Pred.String(), triple.Obj.String())
		} else {
			obj := triple.Obj.String()
			if _, ok := vert_map[obj]; !ok {
				conn.AddVertex(graph, aql.Vertex{Gid: obj})
				vert_map[obj] = 1
			}
			conn.AddEdge(graph, aql.Edge{Src: subj, Dst: obj, Label: triple.Pred.String()})
		}
		if count%1000 == 0 {
			log.Printf("Processed %d triples", count)
		}
		count++
	}
	if cur_vertex != nil {
		conn.AddVertex(graph, *cur_vertex)
	}
	return nil
}

var Cmd = &cobra.Command{
	Use:   "rdf",
	Short: "Loads RDF data",
	Long:  ``,
	RunE:  LoadRDFCmd,
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", "localhost:9090", "Host Server")
	flags.StringVar(&graph, "graph", "default", "Graph")
}
