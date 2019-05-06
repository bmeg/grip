package gripql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bmeg/golib"
	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func openFile(file string) (chan []byte, error) {
	var reader chan []byte
	var err error
	if strings.HasSuffix(file, ".gz") {
		reader, err = golib.ReadGzipLines(file)
	} else {
		reader, err = golib.ReadFileLines(file)
	}
	return reader, err
}

// StreamVerticesFromFile reads a file containing a vertex per line and
// streams *Vertex objects out on a channel
func StreamVerticesFromFile(file string) chan *Vertex {
	vertChan := make(chan *Vertex, 1000)

	reader, err := openFile(file)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Errorf("Reading file: %s", file)
		close(vertChan)
		return vertChan
	}
	m := jsonpb.Unmarshaler{AllowUnknownFields: true}
	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < runtime.NumCPU(); i++ {
		g.Go(func() error {
			for line := range reader {
				v := &Vertex{}
				err := m.Unmarshal(bytes.NewReader(line), v)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling vertex: %v", line)
				}
				vertChan <- v
			}
			return nil
		})
	}

	go func() {
		g.Wait()
		close(vertChan)
	}()

	return vertChan
}

// StreamEdgesFromFile reads a file containing an edge per line and
// streams Edge objects on a channel
func StreamEdgesFromFile(file string) chan *Edge {
	edgeChan := make(chan *Edge, 1000)

	reader, err := openFile(file)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Errorf("Reading file: %s", file)
		close(edgeChan)
		return edgeChan
	}
	m := jsonpb.Unmarshaler{AllowUnknownFields: true}
	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < runtime.NumCPU(); i++ {
		g.Go(func() error {
			for line := range reader {
				e := &Edge{}
				err := m.Unmarshal(bytes.NewReader(line), e)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling edge: %v", line)
				}
				edgeChan <- e
			}
			return nil
		})
	}

	go func() {
		g.Wait()
		close(edgeChan)
	}()

	return edgeChan
}

var m = jsonpb.Marshaler{}

// ParseYAMLGraph parses a YAML doc into the given Graph instance.
func ParseYAMLGraph(raw []byte) ([]*Graph, error) {
	graphs := []*Graph{}
	tmp := []interface{}{}
	err := yaml.Unmarshal(raw, &tmp)
	if err != nil {
		var tmp2 interface{}
		err := yaml.Unmarshal(raw, &tmp2)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, tmp2)
	}
	for _, s := range tmp {
		part, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		g := &Graph{}
		err = jsonpb.UnmarshalString(string(part), g)
		if err != nil {
			return nil, err
		}
		if g.Graph == "" {
			return nil, fmt.Errorf("missing graph name")
		}
		graphs = append(graphs, g)
	}
	return graphs, nil
}

// ParseJSONGraph parses a JSON doc into the given Graph instance.
func ParseJSONGraph(raw []byte) ([]*Graph, error) {
	graphs := []*Graph{}
	tmp := []interface{}{}
	err := json.Unmarshal(raw, &tmp)
	if err != nil {
		var tmp2 interface{}
		err := json.Unmarshal(raw, &tmp2)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, tmp2)
	}
	for _, s := range tmp {
		part, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		g := &Graph{}
		err = jsonpb.UnmarshalString(string(part), g)
		if err != nil {
			return nil, err
		}
		if g.Graph == "" {
			return nil, fmt.Errorf("missing graph name")
		}
		graphs = append(graphs, g)
	}
	return graphs, nil
}

// ParseGraphYAMLFile parses a graph file, which is formatted in YAML,
// and returns a slice of graph objects.
func parseGraphFile(relpath string, format string) ([]*Graph, error) {
	var graphs []*Graph
	var err error

	if relpath == "" {
		return nil, fmt.Errorf("path is empty")
	}

	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}

	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read graph at path %s: \n%v", path, err)
	}

	// Parse file contents
	switch format {
	case "yaml":
		graphs, err = ParseYAMLGraph(source)
	case "json":
		graphs, err = ParseJSONGraph(source)
	default:
		err = fmt.Errorf("unknown file format: %s", format)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse graph at path %s: \n%v", path, err)
	}
	return graphs, nil
}

// ParseYAMLGraphFile parses a graph file, which is formatted in YAML,
// and returns a slice of graph objects.
func ParseYAMLGraphFile(relpath string) ([]*Graph, error) {
	return parseGraphFile(relpath, "yaml")
}

// ParseJSONGraphFile parses a graph file, which is formatted in JSON,
// and returns a slice of graph objects.
func ParseJSONGraphFile(relpath string) ([]*Graph, error) {
	return parseGraphFile(relpath, "json")
}

// GraphToYAMLString returns a graph formatted as a YAML string
func GraphToYAMLString(graph *Graph) (string, error) {
	b := []byte{}
	out := bytes.NewBuffer(b)
	err := m.Marshal(out, graph)
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %v", err)
	}
	sb, err := yaml.JSONToYAML(out.Bytes())
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %v", err)
	}
	return string(sb), nil
}

// GraphToJSONString returns a graph formatted as a JSON string
func GraphToJSONString(graph *Graph) (string, error) {
	m := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: false,
		Indent:       "  ",
		OrigName:     false,
	}
	txt, err := m.MarshalToString(graph)
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %v", err)
	}
	return txt, nil
}
