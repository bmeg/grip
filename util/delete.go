package util

import (
	"fmt"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	multierror "github.com/hashicorp/go-multierror"
)

func DeleteBatch(stream <-chan *gdbi.ElementID, batchSize int, graph string, delVertex func(key string) error, delEdge func(key string) error) error {
	log.Infoln("HELLO WE IN DELETE BATCH FUNC")
	var bulkErr *multierror.Error
	vertCount := 0
	elementIdBatchChan := make(chan []*gdbi.ElementID)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for elemBatch := range elementIdBatchChan {
			for _, elem := range elemBatch {
				if err := delVertex(elem.Id); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
				if err := delEdge(elem.Id); err != nil {
					bulkErr = multierror.Append(bulkErr, err)
				}
			}
		}
	}()

	elementIdBatch := make([]*gdbi.ElementID, 0, batchSize)

	for element := range stream {
		if element.Graph != graph {
			bulkErr = multierror.Append(
				bulkErr,
				fmt.Errorf("unexpected graph reference: %s != %s", element.Graph, graph),
			)
		} else if element.Id != "" {
			if len(elementIdBatch) >= batchSize {
				elementIdBatchChan <- elementIdBatch
				elementIdBatch = make([]*gdbi.ElementID, 0, batchSize)
			}
			elementIdBatch = append(elementIdBatch, element)
			vertCount++
		}
	}

	elementIdBatchChan <- elementIdBatch

	close(elementIdBatchChan)
	wg.Wait()

	if vertCount != 0 {
		fmt.Printf("%d vertices streamed to BulkDelete\n", vertCount)
	}

	return bulkErr.ErrorOrNil()
}
