package rdf

import (
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/knakk/rdf"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var host = "localhost:8202"
var dump bool
var graph string
var gzipInput bool
var uMap = map[string]string{}

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

type grpcEmitter struct {
	client   gripql.Client
	elemChan chan *gripql.GraphElement
}

func newGRPCEmitter(client gripql.Client) emitter {
	elemChan := make(chan *gripql.GraphElement)
	go func() {
		if err := client.BulkAdd(elemChan); err != nil {
			log.Errorf("bulk add error: %v", err)
		}
	}()
	return grpcEmitter{client, elemChan}
}

func (ge grpcEmitter) AddEdge(graph string, e *gripql.Edge) error {
	ge.elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
	return nil
}

func (ge grpcEmitter) AddVertex(graph string, v *gripql.Vertex) error {
	ge.elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
	return nil
}

func (ge grpcEmitter) Close() {
	close(ge.elemChan)
}

type gElement struct {
	vertex *gripql.Vertex
	edge   *gripql.Edge
}

func stringClean(cMap map[string]string, s string) string {
	for k, v := range cMap {
		if strings.HasPrefix(s, k) {
			return v + s[len(k):]
		}
	}
	return s
}

//LoadRDFCmd is the main command line for loading RDF data
func LoadRDFCmd(cmd *cobra.Command, args []string) error {
	graph = args[0]
	log.Infof("Loading data into graph: %s", graph)

	// log.Printf("%s", uMap)
	// return nil

	f, err := os.Open(args[1])
	if err != nil {
		log.Errorf("Error: %v", err)
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
	if !dump {
		conn, err := gripql.Connect(rpc.ConfigWithDefaults(host), true)
		if err != nil {
			log.Errorf("Error: %v", err)
			os.Exit(1)
		}
		emit = newGRPCEmitter(conn)
	} else {
		emit = newFileEmitter(graph)
	}

	count := 0
	dec := rdf.NewTripleDecoder(reader, rdf.RDFXML)
	var curVertex *gripql.Vertex
	curSubj := ""
	tripleChan := make(chan rdf.Triple, 1000)
	go func() {
		for triple, err := dec.Decode(); err != io.EOF; triple, err = dec.Decode() {
			if err == nil {
				tripleChan <- triple
			}
		}
		close(tripleChan)
	}()
	elementChan := make(chan gElement, 1000)
	go func() {
		for triple := range tripleChan {
			subj := stringClean(uMap, triple.Subj.String())
			if subj != curSubj && curVertex != nil {
				elementChan <- gElement{vertex: curVertex}
				curVertex = nil
			}
			curSubj = subj
			if triple.Obj.Type() == rdf.TermLiteral {
				if curVertex == nil {
					curVertex = &gripql.Vertex{Gid: subj}
				}
				curVertex.SetProperty(stringClean(uMap, triple.Pred.String()), triple.Obj.String())
			} else if triple.Pred.String() == RdfType {
				if curVertex == nil {
					curVertex = &gripql.Vertex{Gid: subj}
				}
				curVertex.Label = stringClean(uMap, triple.Obj.String())
			} else {
				obj := stringClean(uMap, triple.Obj.String())
				elementChan <- gElement{edge: &gripql.Edge{From: subj, To: obj, Label: stringClean(uMap, triple.Pred.String())}}
			}
			if count%10000 == 0 {
				log.Infof("Processed %d triples", count)
			}
			count++
		}
		if curVertex != nil {
			elementChan <- gElement{vertex: curVertex}
		}
		close(elementChan)
	}()
	for element := range elementChan {
		if element.vertex != nil {
			if element.vertex.Gid != "" && element.vertex.Label != "" {
				err := emit.AddVertex(graph, element.vertex)
				if err != nil {
					log.Errorf("%s", err)
				}
			}
		} else if element.edge != nil {
			if element.edge.To != "" && element.edge.From != "" && element.edge.Label != "" {
				err := emit.AddEdge(graph, element.edge)
				if err != nil {
					log.Errorf("%s", err)
				}
			}
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
	flags.BoolVar(&dump, "dump", false, "dump to file")
	flags.BoolVar(&gzipInput, "gzip", false, "gziped input file")
	flags.StringVar(&host, "host", host, "grip server url")
	flags.StringToStringVarP(&uMap, "map", "m", map[string]string{}, "URLMap: -m src=dst")
}
