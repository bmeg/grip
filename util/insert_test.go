package util

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/gdbi"
	multierror "github.com/hashicorp/go-multierror"
)

func TestBatchInsertValidation(t *testing.T) {
	elems := []*gdbi.GraphElement{
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v1", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v2", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v3", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v4", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v5", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v6", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v7", Label: "test"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e1", Label: "test", From: "v1", To: "v2"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e2", Label: "test", From: "v2", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e3", Label: "test", From: "v3", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e4", Label: "test", From: "v4", To: "v3"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e5", Label: "test", From: "v5", To: "v3"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e6", Label: "test", From: "v6", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e7", Label: "test", From: "v7", To: "v2"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e8", Label: "test", From: "v1", To: "v7"}},
	}

	vAdd := func([]*gdbi.Vertex) error {
		return nil
	}
	eAdd := func([]*gdbi.Edge) error {
		return nil
	}

	i := make(chan *gdbi.GraphElement)

	go func() {
		for _, e := range elems {
			i <- e
		}
		close(i)
	}()

	err := StreamBatch(i, 5, "graph", vAdd, eAdd)

	if err != nil {
		t.Error(err)
	}
}

func TestBatchGraphValidation(t *testing.T) {
	elems := []*gdbi.GraphElement{
		{Graph: "graph1", Vertex: &gdbi.Vertex{ID: "v1", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v3"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v4", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v5", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v6", Label: "test"}},
		{Graph: "graph", Vertex: &gdbi.Vertex{ID: "v7", Label: "test"}},
		{Graph: "graph1", Edge: &gdbi.Edge{ID: "e1", Label: "test", From: "v1", To: "v2"}},
		{Graph: "graph3", Edge: &gdbi.Edge{ID: "e2", Label: "test", From: "v2", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e3", Label: "test", From: "v3", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e4", Label: "test", To: "v3"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e5", Label: "test", From: "v5"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e6", Label: "test", From: "v6", To: "v1"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e7", Label: "test", From: "v7", To: "v2"}},
		{Graph: "graph", Edge: &gdbi.Edge{ID: "e8", Label: "test", From: "v1", To: "v7"}},
	}

	vAdd := func([]*gdbi.Vertex) error {
		return nil
	}
	eAdd := func(e []*gdbi.Edge) error {
		return fmt.Errorf("edgeAdd test error")
	}

	i := make(chan *gdbi.GraphElement)

	go func() {
		for _, e := range elems {
			i <- e
		}
		close(i)
	}()

	err := StreamBatch(i, 5, "graph", vAdd, eAdd)

	if merr, ok := err.(*multierror.Error); ok {
		if len(merr.Errors) != 8 {
			t.Log(merr.Error())
			t.Errorf("incorrect number of errors returned")
		}
	} else {
		t.Errorf("expected err of type *multierror.Error")
	}
}
