package util

import (
	"fmt"
	"io"

	"github.com/bmeg/grip/gripql"
	log "github.com/sirupsen/logrus"
)

type graphElementArray struct {
	vertices []*gripql.Vertex
	edges    []*gripql.Edge
}

func newGraphElementArray(vertexBufSize, edgeBufSize int) *graphElementArray {
	if vertexBufSize != 0 {
		return &graphElementArray{vertices: make([]*gripql.Vertex, 0, vertexBufSize)}
	}
	if edgeBufSize != 0 {
		return &graphElementArray{edges: make([]*gripql.Edge, 0, edgeBufSize)}
	}
	return nil
}

// SteamBatch a stream of inputs and loads them into the graph
// This function assumes incoming stream is GraphElemnts from a single graph
func SteamBatch(stream <-chan *gripql.GraphElement, batchSize int, vertexAdd func([]*gripql.Vertex) error, edgeAdd func([]*gripql.Edge) error) error {

	vertCount := 0
	edgeCount := 0

	vertexBatchChan := make(chan *graphElementArray)
	edgeBatchChan := make(chan *graphElementArray)
	closeChan := make(chan bool)

	go func() {
		for vBatch := range vertexBatchChan {
			if len(vBatch.vertices) > 0 {
				err := vertexAdd(vBatch.vertices)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: add vertex error")
				}
			}
		}
		closeChan <- true
	}()

	go func() {
		for eBatch := range edgeBatchChan {
			if len(eBatch.edges) > 0 {
				err := edgeAdd(eBatch.edges)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: add edge error")
				}
			}
		}
		closeChan <- true
	}()

	vertexBatch := newGraphElementArray(batchSize, 0)
	edgeBatch := newGraphElementArray(0, batchSize)
	var loopErr error
	for element := range stream {
		if element.Vertex != nil {
			if len(vertexBatch.vertices) >= batchSize {
				vertexBatchChan <- vertexBatch
				vertexBatch = newGraphElementArray(batchSize, 0)
			}
			vertex := element.Vertex
			err := vertex.Validate()
			if err != nil {
				return fmt.Errorf("vertex validation failed: %v", err)
			}
			vertexBatch.vertices = append(vertexBatch.vertices, vertex)
			vertCount++
		} else if element.Edge != nil {
			if len(edgeBatch.edges) >= batchSize {
				edgeBatchChan <- edgeBatch
				edgeBatch = newGraphElementArray(0, batchSize)
			}
			edge := element.Edge
			if edge.Gid == "" {
				edge.Gid = UUID()
			}
			err := edge.Validate()
			if err != nil {
				return fmt.Errorf("edge validation failed: %v", err)
			}
			edgeBatch.edges = append(edgeBatch.edges, edge)
			edgeCount++
		}
	}

	vertexBatchChan <- vertexBatch
	edgeBatchChan <- edgeBatch

	if vertCount != 0 {
		log.Debugf("%d vertices streamed", vertCount)
	}
	if edgeCount != 0 {
		log.Debugf("%d edges streamed", edgeCount)
	}

	close(edgeBatchChan)
	close(vertexBatchChan)
	<-closeChan
	<-closeChan

	if loopErr != io.EOF {
		return loopErr
	}
	return nil
}
