package util

import (
	"github.com/bmeg/grip/gripql"
	"testing"
)

func TestBatchInsertValidation(t *testing.T) {
	elems := []gripql.GraphElement{
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v1", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v2", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v3", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v4", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v5", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v6", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v7", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e1", Label: "test", From: "v1", To: "v2"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e2", Label: "test", From: "v2", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e3", Label: "test", From: "v3", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e4", Label: "test", From: "v4", To: "v3"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e5", Label: "test", From: "v5", To: "v3"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e6", Label: "test", From: "v6", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e7", Label: "test", From: "v7", To: "v2"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e8", Label: "test", From: "v1", To: "v7"}},
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
			i <- &e
		}
		close(i)
	}()

	err := SteamBatch(i, 5, vAdd, eAdd)

	if err != nil {
		t.Error(err)
	}

}

func TestBatchFromValidation(t *testing.T) {
	elems := []gripql.GraphElement{
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v1", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v2", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v3", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v4", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v5", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v6", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Vertex: &gripql.Vertex{Gid: "v7", Label: "test"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e1", Label: "test", To: "v2"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e2", Label: "test", From: "v2", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e3", Label: "test", From: "v3", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e4", Label: "test", From: "v4", To: "v3"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e5", Label: "test", From: "v5", To: "v3"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e6", Label: "test", From: "v6", To: "v1"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e7", Label: "test", From: "v7", To: "v2"}},
		gripql.GraphElement{Graph: "graph", Edge: &gripql.Edge{Gid: "e8", Label: "test", To: "v7"}},
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
			i <- &e
		}
		close(i)
	}()

	err := SteamBatch(i, 5, vAdd, eAdd)

	if err == nil {
		t.Errorf("Validation error not returned")
	}

}
