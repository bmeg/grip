package rdf

import (
	"compress/gzip"
	"io"
	"log"
	"os"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/knakk/rdf"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var dump = ""
var graph string
var gzipInput bool

// RdfType is used to define the label of a vertex
var RdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

type emitter interface {
	AddVertex(string, *gripql.Vertex) error
	AddEdge(string, *gripql.Edge) error
	Close()
}

type fileEmitter struct {
	vertexHandle io.WriteCloser
	edgeHandle   io.WriteCloser
	jm           jsonpb.Marshaler
}

func (fe fileEmitter) AddVertex(graph string, v *gripql.Vertex) error {
	err := fe.jm.Marshal(fe.vertexHandle, v)
	if err != nil {
		return err
	}
	fe.vertexHandle.Write([]byte("\n"))
	return nil
}

func (fe fileEmitter) AddEdge(graph string, e *gripql.Edge) error {
	err := fe.jm.Marshal(fe.edgeHandle, e)
	if err != nil {
		return err
	}
	fe.edgeHandle.Write([]byte("\n"))
	return nil
}

func (fe fileEmitter) Close() {
	fe.vertexHandle.Close()
	fe.edgeHandle.Close()
}

func newFileEmitter(path string) emitter {
	vertexFile, _ := os.Create(path + ".vertex.json")
	edgeFile, _ := os.Create(path + ".edge.json")
	jm := jsonpb.Marshaler{}
	return fileEmitter{vertexFile, edgeFile, jm}
}

//LoadRDFCmd is the main command line for loading RDF data
func LoadRDFCmd(cmd *cobra.Command, args []string) error {
	graph = args[0]
	log.Println("Loading data into graph:", graph)

	f, err := os.Open(args[1])
	if err != nil {
		log.Printf("Error: %s", err)
		os.Exit(1)
	}
	var reader io.Reader
	if gzipInput {
		fz, _ := gzip.NewReader(f)
		reader = fz
	} else {
		reader = f
	}

	var emit emitter
	if dump == "" {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			log.Printf("%s", err)
			os.Exit(1)
		}
		emit = conn
	} else {
		emit = newFileEmitter(dump)
	}

	count := 0
	dec := rdf.NewTripleDecoder(reader, rdf.RDFXML)
	var curVertex *gripql.Vertex
	curSubj := ""
	for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
		subj := triple.Subj.String()
		if subj != curSubj && curVertex != nil {
			err := emit.AddVertex(graph, curVertex)
			if err != nil {
				return err
			}
			curVertex = nil
		}
		curSubj = subj

		if triple.Obj.Type() == rdf.TermLiteral {
			if curVertex == nil {
				curVertex = &gripql.Vertex{Gid: subj}
			}
			curVertex.SetProperty(triple.Pred.String(), triple.Obj.String())
		} else if triple.Pred.String() == RdfType {
			if curVertex == nil {
				curVertex = &gripql.Vertex{Gid: subj}
			}
			curVertex.Label = triple.Obj.String()
		} else {
			obj := triple.Obj.String()
			err := emit.AddEdge(graph, &gripql.Edge{From: subj, To: obj, Label: triple.Pred.String()})
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
		err := emit.AddVertex(graph, curVertex)
		if err != nil {
			return err
		}
	}
	emit.Close()
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
	flags.StringVar(&dump, "dump", "", "dump to files")
	flags.BoolVar(&gzipInput, "gzip", false, "gziped input file")
	flags.StringVar(&host, "host", host, "grip server url")
}
