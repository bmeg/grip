package esql

import (
	"errors"
	"context"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/gripql"
)

// AddVertexIndex add index to vertices
func (g *Graph) AddVertexIndex(field string) error {
	return errors.New("not implemented")
}

// DeleteVertexIndex delete index from vertices
func (g *Graph) DeleteVertexIndex(field string) error {
	return errors.New("not implemented")
}

// GetVertexIndexList lists indices
func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	o := make(chan *gripql.IndexID)
	defer close(o)
	return o
}


func (g *Graph) VertexIndexScan(ctx context.Context, query *gripql.IndexQuery) <-chan string {
	log.Errorf("VertexIndexScan not implemented for PSQL")
	o := make(chan string)
	defer close(o)
	return o
}
