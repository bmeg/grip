package util

import (
	"fmt"
	"testing"

	"github.com/bmeg/grip/gripql"
	multierror "github.com/hashicorp/go-multierror"
)

func TestBatchInsertValidation(t *testing.T) {
	elems := []*gripql.GraphElement{
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v1", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v2", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v3", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v4", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v5", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v6", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v7", Label: "test"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e1", Label: "test", From: "v1", To: "v2"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e2", Label: "test", From: "v2", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e3", Label: "test", From: "v3", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e4", Label: "test", From: "v4", To: "v3"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e5", Label: "test", From: "v5", To: "v3"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e6", Label: "test", From: "v6", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e7", Label: "test", From: "v7", To: "v2"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e8", Label: "test", From: "v1", To: "v7"}},
	}

	vAdd := func([]*gripql.Vertex) error {
		return nil
	}
	eAdd := func([]*gripql.Edge) error {
		return nil
	}

	i := make(chan *gripql.GraphElement)

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
	elems := []*gripql.GraphElement{
		{Graph: "graph1", Vertex: &gripql.Vertex{Gid: "v1", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v3"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v4", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v5", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v6", Label: "test"}},
		{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v7", Label: "test"}},
		{Graph: "graph1", Edge: &gripql.Edge{Gid: "e1", Label: "test", From: "v1", To: "v2"}},
		{Graph: "graph3", Edge: &gripql.Edge{Gid: "e2", Label: "test", From: "v2", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e3", Label: "test", From: "v3", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e4", Label: "test", To: "v3"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e5", Label: "test", From: "v5"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e6", Label: "test", From: "v6", To: "v1"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e7", Label: "test", From: "v7", To: "v2"}},
		{Graph: "graph", Edge: &gripql.Edge{Gid: "e8", Label: "test", From: "v1", To: "v7"}},
	}

	vAdd := func([]*gripql.Vertex) error {
		return nil
	}
	eAdd := func(e []*gripql.Edge) error {
		return fmt.Errorf("edgeAdd test error")
	}

	i := make(chan *gripql.GraphElement)

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
