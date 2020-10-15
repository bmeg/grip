package util

import (
	"fmt"
	"sync"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"
)

// StreamBatch a stream of inputs and loads them into the graph
// This function assumes incoming stream is GraphElemnts from a single graph
func StreamBatch(stream <-chan *gripql.GraphElement, batchSize int, graph string, vertexAdd func([]*gripql.Vertex) error, edgeAdd func([]*gripql.Edge) error) error {

	var bulkErr *multierror.Error
	vertCount := 0
	edgeCount := 0
	vertexBatchChan := make(chan []*gripql.Vertex)
	edgeBatchChan := make(chan []*gripql.Edge)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for vBatch := range vertexBatchChan {
			if len(vBatch) > 0 {
				err := vertexAdd(vBatch)
				if err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for eBatch := range edgeBatchChan {
			if len(eBatch) > 0 {
				err := edgeAdd(eBatch)
				if err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
		}
		wg.Done()
	}()

	vertexBatch := make([]*gripql.Vertex, 0, batchSize)
	edgeBatch := make([]*gripql.Edge, 0, batchSize)

	for element := range stream {
		if element.Graph != graph {
			bulkErr = multierror.Append(
				bulkErr,
				fmt.Errorf("unexpected graph reference: %s != %s", element.Graph, graph),
			)
		} else if element.Vertex != nil {
			if len(vertexBatch) >= batchSize {
				vertexBatchChan <- vertexBatch
				vertexBatch = make([]*gripql.Vertex, 0, batchSize)
			}
			vertex := element.Vertex
			err := vertex.Validate()
			if err != nil {
				bulkErr = multierror.Append(
					bulkErr,
					fmt.Errorf("vertex validation failed: %v", err),
				)
			} else {
				vertexBatch = append(vertexBatch, vertex)
				vertCount++
			}
		} else if element.Edge != nil {
			if len(edgeBatch) >= batchSize {
				edgeBatchChan <- edgeBatch
				edgeBatch = make([]*gripql.Edge, 0, batchSize)
			}
			edge := element.Edge
			if edge.Gid == "" {
				edge.Gid = UUID()
			}
			err := edge.Validate()
			if err != nil {
				bulkErr = multierror.Append(
					bulkErr,
					fmt.Errorf("edge validation failed: %v", err),
				)
			} else {
				edgeBatch = append(edgeBatch, edge)
				edgeCount++
			}
		}
	}
	vertexBatchChan <- vertexBatch
	edgeBatchChan <- edgeBatch

	if vertCount != 0 {
		log.Debugf("%d vertices streamed to BulkAdd", vertCount)
	}

	if edgeCount != 0 {
		log.Debugf("%d edges streamed to BulkAdd", edgeCount)
	}

	close(edgeBatchChan)
	close(vertexBatchChan)
	wg.Wait()

	return bulkErr.ErrorOrNil()
}
