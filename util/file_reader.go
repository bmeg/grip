package util

import (
	"bytes"
	"io"
	"log"

	"github.com/bmeg/golib"
	"github.com/bmeg/grip/gripql"
	"github.com/golang/protobuf/jsonpb"
)

// StreamVerticesFromFile reads a file containing a vertex per line and
// streams *gripql.Vertex objects out on a channel
func StreamVerticesFromFile(file string) chan *gripql.Vertex {
	vertChan := make(chan *gripql.Vertex, 100)

	go func() {
		defer close(vertChan)

		reader, err := golib.ReadFileLines(file)
		if err != nil {
			log.Printf("Error: reading file: %v", err)
			return
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		for line := range reader {
			v := &gripql.Vertex{}
			err := m.Unmarshal(bytes.NewReader(line), v)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error: unmarshaling vertex: %v", err)
				return
			}
			vertChan <- v
		}
	}()

	return vertChan
}

// StreamEdgesFromFile reads a file containing an edge per line and
// streams gripql.Edge objects on a channel
func StreamEdgesFromFile(file string) chan *gripql.Edge {
	edgeChan := make(chan *gripql.Edge, 100)

	go func() {
		defer close(edgeChan)

		reader, err := golib.ReadFileLines(file)
		if err != nil {
			log.Printf("Error: reading file: %v", err)
			return
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		for line := range reader {
			e := &gripql.Edge{}
			err := m.Unmarshal(bytes.NewReader(line), e)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error: unmarshaling edge: %v", err)
				return
			}
			edgeChan <- e
		}
	}()

	return edgeChan
}
