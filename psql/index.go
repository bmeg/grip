package psql

import (
	"errors"

	"github.com/bmeg/grip/gripql"
)

// AddVertexIndex add index to vertices
func (g *Graph) AddVertexIndex(label string, field string) error {
	return errors.New("not implemented")
}

// DeleteVertexIndex delete index from vertices
func (g *Graph) DeleteVertexIndex(label string, field string) error {
	return errors.New("not implemented")
}

// GetVertexIndexList lists indices
func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	o := make(chan *gripql.IndexID)
	defer close(o)
	return o
}
