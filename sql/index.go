package sql

import (
	"context"
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
func (g *Graph) GetVertexIndexList() chan gripql.IndexID {
	o := make(chan gripql.IndexID)
	go func() {
		defer close(o)
	}()
	return o
}

// GetVertexTermAggregation get count of every term across vertices
func (g *Graph) GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*gripql.AggregationResult, error) {
	return nil, errors.New("not implemented")
}

// GetVertexHistogramAggregation get binned counts of a term across vertices
func (g *Graph) GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*gripql.AggregationResult, error) {
	return nil, errors.New("not implemented")
}

// GetVertexPercentileAggregation get percentiles of a term across vertices
func (g *Graph) GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*gripql.AggregationResult, error) {
	return nil, errors.New("not implemented")
}
