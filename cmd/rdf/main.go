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

var host = "localhost:8202"
var graph = "default"

//LoadRDFCmd is the main command line for loading RDF data
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

	vertMap := map[string]int{}

	count := 0
	fz, _ := gzip.NewReader(f)
	dec := rdf.NewTripleDecoder(fz, rdf.RDFXML)
	var curVertex *aql.Vertex
	curSubj := ""
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		subj := triple.Subj.String()
		if subj != curSubj && curVertex != nil {
			conn.AddVertex(graph, *curVertex)
			curVertex = nil
		}
		curSubj = subj
		if _, ok := vertMap[subj]; !ok {
			conn.AddVertex(graph, aql.Vertex{Gid: subj})
			vertMap[subj] = 1
		}
		if triple.Obj.Type() == rdf.TermLiteral {
			if curVertex == nil {
				curVertex = &aql.Vertex{Gid: subj}
			}
			curVertex.SetProperty(triple.Pred.String(), triple.Obj.String())
		} else {
			obj := triple.Obj.String()
			if _, ok := vertMap[obj]; !ok {
				conn.AddVertex(graph, aql.Vertex{Gid: obj})
				vertMap[obj] = 1
			}
			conn.AddEdge(graph, aql.Edge{From: subj, To: obj, Label: triple.Pred.String()})
		}
		if count%1000 == 0 {
			log.Printf("Processed %d triples", count)
		}
		count++
	}
	if curVertex != nil {
		conn.AddVertex(graph, *curVertex)
	}
	return nil
}

// Cmd is the declaration for cobra of the command line
var Cmd = &cobra.Command{
	Use:   "rdf",
	Short: "Loads RDF data",
	Long:  ``,
	RunE:  LoadRDFCmd,
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "Host Server")
	flags.StringVar(&graph, "graph", "default", "Graph")
}
