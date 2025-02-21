package util

import (
	"fmt"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"
)

// StreamBatch a stream of inputs and loads them into the graph
// This function assumes incoming stream is GraphElemnts from a single graph
func StreamBatch(stream <-chan *gdbi.GraphElement, batchSize int, graph string, vertexAdd func(<-chan *gdbi.Vertex, int) error, edgeAdd func(<-chan *gdbi.Edge, int) error) error {
	var bulkErr *multierror.Error
	vertCount := 0
	edgeCount := 0
	wg := &sync.WaitGroup{}

	vertexChan := make(chan *gdbi.Vertex, batchSize)
	edgeChan := make(chan *gdbi.Edge, batchSize)

	// Start goroutines to process vertices and edges
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := vertexAdd(vertexChan, batchSize); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := edgeAdd(edgeChan, batchSize); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}()

	vertexBatch := make([]*gdbi.Vertex, 0, batchSize)
	edgeBatch := make([]*gdbi.Edge, 0, batchSize)

	for element := range stream {
		if element.Graph != graph {
			bulkErr = multierror.Append(
				bulkErr,
				fmt.Errorf("unexpected graph reference: %s != %s", element.Graph, graph),
			)
			continue
		}
		if element.Vertex != nil {
			vertex := element.Vertex
			if err := vertex.Validate(); err != nil {
				bulkErr = multierror.Append(
					bulkErr,
					fmt.Errorf("vertex validation failed: %v", err),
				)
				continue
			}

			vertexBatch = append(vertexBatch, vertex)
			vertCount++

			if len(vertexBatch) >= batchSize {
				for _, v := range vertexBatch {
					vertexChan <- v
				}
				vertexBatch = vertexBatch[:0] // Reset batch slice
			}
		} else if element.Edge != nil {
			edge := element.Edge
			if edge.ID == "" {
				edge.ID = UUID()
			}

			if err := edge.Validate(); err != nil {
				bulkErr = multierror.Append(
					bulkErr,
					fmt.Errorf("edge validation failed: %v", err),
				)
				continue
			}

			edgeBatch = append(edgeBatch, edge)
			edgeCount++

			if len(edgeBatch) >= batchSize {
				for _, e := range edgeBatch {
					edgeChan <- e
				}
				edgeBatch = edgeBatch[:0] // Reset batch slice
			}
		}
	}

	// Send remaining vertices and edges in the batch
	for _, v := range vertexBatch {
		vertexChan <- v
	}
	for _, e := range edgeBatch {
		edgeChan <- e
	}

	// Close channels after all data is sent
	close(vertexChan)
	close(edgeChan)

	wg.Wait()

	if vertCount != 0 {
		log.Debugf("%d vertices streamed to BulkAdd", vertCount)
	}

	if edgeCount != 0 {
		log.Debugf("%d edges streamed to BulkAdd", edgeCount)
	}

	return bulkErr.ErrorOrNil()
}
