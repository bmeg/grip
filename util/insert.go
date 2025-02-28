package util

import (
	"context"
	"fmt"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"
	"golang.org/x/sync/semaphore"
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

	sem := semaphore.NewWeighted(int64(batchSize * 2))

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

			sem.Acquire(context.Background(), 1)
			vertexBatch = append(vertexBatch, vertex)
			vertCount++

			if len(vertexBatch) >= batchSize {
				batchSizeToRelease := len(vertexBatch)
				for _, v := range vertexBatch {
					vertexChan <- v
				}
				vertexBatch = make([]*gdbi.Vertex, 0, batchSize)
				sem.Release(int64(batchSizeToRelease))
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

			sem.Acquire(context.Background(), 1)
			edgeBatch = append(edgeBatch, edge)
			edgeCount++

			if len(edgeBatch) >= batchSize {
				batchSizeToRelease := len(edgeBatch)
				for _, e := range edgeBatch {
					edgeChan <- e
				}
				edgeBatch = make([]*gdbi.Edge, 0, batchSize)
				sem.Release(int64(batchSizeToRelease))
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
	sem.Release(int64(len(vertexBatch) + len(edgeBatch)))

	if vertCount != 0 {
		log.Debugf("%d vertices streamed to BulkAdd", vertCount)
	}

	if edgeCount != 0 {
		log.Debugf("%d edges streamed to BulkAdd", edgeCount)
	}

	return bulkErr.ErrorOrNil()
}
