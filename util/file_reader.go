package util

import (
	"bytes"
	"fmt"
	"io"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/golib"
	"github.com/golang/protobuf/jsonpb"
)

// StreamVerticesFromFile reads a file containing a vertex per line and
// streams *aql.Vertex objects and errors out on channels
func StreamVerticesFromFile(file string) (chan *aql.Vertex, chan error) {
	vertChan := make(chan *aql.Vertex)
	errChan := make(chan error)

	go func() {
		defer close(vertChan)
		defer close(errChan)

		reader, err := golib.ReadFileLines(file)
		if err != nil {
			errChan <- fmt.Errorf("reading file: %v", err)
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		for line := range reader {
			v := &aql.Vertex{}
			err := m.Unmarshal(bytes.NewReader(line), v)
			if err == io.EOF {
				break
			}
			if err != nil {
				errChan <- fmt.Errorf("unmarshaling vertex: %v", err)
				continue
			}
			vertChan <- v
		}
	}()

	return vertChan, errChan
}

// StreamEdgesFromFile reads a file containing an edge per line and
// streams aql.Edge objects and errors out on channels
func StreamEdgesFromFile(file string) (chan *aql.Edge, chan error) {
	edgeChan := make(chan *aql.Edge)
	errChan := make(chan error)

	go func() {
		defer close(edgeChan)
		defer close(errChan)

		reader, err := golib.ReadFileLines(file)
		if err != nil {
			errChan <- fmt.Errorf("reading file: %v", err)
		}

		m := jsonpb.Unmarshaler{AllowUnknownFields: true}
		for line := range reader {
			e := &aql.Edge{}
			err := m.Unmarshal(bytes.NewReader(line), e)
			if err == io.EOF {
				break
			}
			if err != nil {
				errChan <- fmt.Errorf("unmarshaling edge: %v", err)
				continue
			}
			edgeChan <- e
		}
	}()

	return edgeChan, errChan
}
