package util

import (
	"bufio"
	"compress/gzip"
	"os"
	"strings"
	"sync"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"

	"google.golang.org/protobuf/encoding/protojson"
)

// StreamLines returns a channel of lines from a file.
func StreamLines(file string, chanSize int) (chan string, error) {
	fh, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	var scanner *bufio.Scanner

	if strings.HasSuffix(file, ".gz") {
		gz, err := gzip.NewReader(fh)
		if err != nil {
			return nil, err
		}
		scanner = bufio.NewScanner(gz)
	} else {
		scanner = bufio.NewScanner(fh)
	}

	const maxCapacity = 16 * 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	lineChan := make(chan string, chanSize)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			lineChan <- line
		}
		if err := scanner.Err(); err != nil {
			log.WithFields(log.Fields{"error": err}).Errorf("Reading file: %s", file)
		}
		close(lineChan)
		fh.Close()
	}()

	return lineChan, nil
}

// StreamVerticesFromFile reads a file containing a vertex per line and
// streams *gripql.Vertex objects out on a channel
func StreamVerticesFromFile(file string, workers int) (chan *gripql.Vertex, error) {
	if workers < 1 {
		workers = 1
	}
	if workers > 99 {
		workers = 99
	}
	lineChan, err := StreamLines(file, workers)
	if err != nil {
		return nil, err
	}

	vertChan := make(chan *gripql.Vertex, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for line := range lineChan {
				v := &gripql.Vertex{}
				err := protojson.Unmarshal([]byte(line), v)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling vertex: %s", line)
				} else {
					vertChan <- v
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(vertChan)
	}()

	return vertChan, nil
}

// StreamEdgesFromFile reads a file containing an edge per line and
// streams gripql.Edge objects on a channel
func StreamEdgesFromFile(file string, workers int) (chan *gripql.Edge, error) {
	if workers < 1 {
		workers = 1
	}
	if workers > 99 {
		workers = 99
	}
	lineChan, err := StreamLines(file, workers)
	if err != nil {
		return nil, err
	}

	edgeChan := make(chan *gripql.Edge, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for line := range lineChan {
				e := &gripql.Edge{}
				err := protojson.Unmarshal([]byte(line), e)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling edge: %s", line)
				} else {
					edgeChan <- e
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(edgeChan)
	}()

	return edgeChan, nil
}
