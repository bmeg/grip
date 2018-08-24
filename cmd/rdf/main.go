package rdf

import (
	"compress/gzip"
	"io"
	"log"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/knakk/rdf"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var graph string

//LoadRDFCmd is the main command line for loading RDF data
func LoadRDFCmd(cmd *cobra.Command, args []string) error {
	graph = args[0]
	log.Println("Loading data into graph:", graph)

	f, err := os.Open(args[1])
	if err != nil {
		log.Printf("Error: %s", err)
		os.Exit(1)
	}
	conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
	if err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

	vertMap := map[string]int{}

	count := 0
	fz, _ := gzip.NewReader(f)
	dec := rdf.NewTripleDecoder(fz, rdf.RDFXML)
	var curVertex *gripql.Vertex
	curSubj := ""
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		subj := triple.Subj.String()
		if subj != curSubj && curVertex != nil {
			err := conn.AddVertex(graph, curVertex)
			if err != nil {
				return err
			}
			curVertex = nil
		}
		curSubj = subj
		if _, ok := vertMap[subj]; !ok {
			err := conn.AddVertex(graph, &gripql.Vertex{Gid: subj})
			if err != nil {
				return err
			}
			vertMap[subj] = 1
		}
		if triple.Obj.Type() == rdf.TermLiteral {
			if curVertex == nil {
				curVertex = &gripql.Vertex{Gid: subj}
			}
			curVertex.SetProperty(triple.Pred.String(), triple.Obj.String())
		} else {
			obj := triple.Obj.String()
			if _, ok := vertMap[obj]; !ok {
				err := conn.AddVertex(graph, &gripql.Vertex{Gid: obj})
				if err != nil {
					return err
				}
				vertMap[obj] = 1
			}
			err := conn.AddEdge(graph, &gripql.Edge{From: subj, To: obj, Label: triple.Pred.String()})
			if err != nil {
				return err
			}
		}
		if count%1000 == 0 {
			log.Printf("Processed %d triples", count)
		}
		count++
	}
	if curVertex != nil {
		err := conn.AddVertex(graph, curVertex)
		if err != nil {
			return err
		}
	}
	return nil
}

// Cmd is the declaration for cobra of the command line
var Cmd = &cobra.Command{
	Use:   "rdf <graph> <RDF file>",
	Short: "Loads RDF data into a graph",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	RunE:  LoadRDFCmd,
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&host, "host", host, "grip server url")
}
