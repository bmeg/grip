package util

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strings"

	"github.com/bmeg/golib"
	"github.com/bmeg/grip/gripql"
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
// streams *gripql.Vertex objects out on a channel
func StreamVerticesFromFile(file string) chan *gripql.Vertex {
	vertChan := make(chan *gripql.Vertex, 1000)

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
				v := &gripql.Vertex{}
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
// streams gripql.Edge objects on a channel
func StreamEdgesFromFile(file string) chan *gripql.Edge {
	edgeChan := make(chan *gripql.Edge, 1000)

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
				e := &gripql.Edge{}
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
