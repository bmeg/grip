package engine

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/memgraph"
  "github.com/bmeg/arachne/protoutil"
	"github.com/go-test/deep"
	"testing"
	"time"
)

type dat map[string]interface{}
var db = memgraph.NewMemGraph()

func vert(id, label string, d dat) *traveler {
  v := &aql.Vertex{
    Gid: id,
    Label: label,
    Data: protoutil.AsStruct(d),
  }
  db.SetVertex(v)
  return &traveler{
    id: id,
    label: label,
    dataType: vertexData,
    data: d,
  }
}

func edge(id, from, to, label string, d dat) *traveler {
  v := &aql.Edge{
    Gid: id,
    From: from,
    To: to,
    Label: label,
    Data: protoutil.AsStruct(d),
  }
  db.SetEdge(v)
  return &traveler{
    id: id,
    label: label,
    dataType: edgeData,
    data: d,
  }
}

var verts = []*traveler{
  vert("v0", "Human", dat{"name": "Alex"}),
  vert("v1", "Human", dat{"name": "Kyle"}),
  vert("v2", "Human", dat{"name": "Ryan"}),
  vert("v3", "Robot", dat{"name": "C-3PO"}),
  vert("v4", "Robot", dat{"name": "R2-D2"}),
  vert("v5", "Robot", dat{"name": "Bender"}),
  vert("v6", "Clone", dat{"name": "Alex"}),
  vert("v7", "Clone", dat{"name": "Kyle"}),
  vert("v8", "Clone", dat{"name": "Ryan"}),
  vert("v9", "Clone", nil),
  vert("v10", "Project", dat{"name": "Funnel"}),
}

var edges = []*traveler{
  edge("e0", "v0", "v10", "WorksOn", nil),
}

func nq() *aql.Query {
  return aql.NewQuery("test")
}

var table = []struct {
  query *aql.Query
	expected []*traveler
}{
	{
    nq().V().Has("name", "Kyle", "Alex"),
		pick(verts, 0, 1, 6, 7),
	},
	{
    nq().V().Has("non-existant", "Kyle", "Alex"),
    pick(verts),
	},
	{
    nq().V().HasLabel("Human"),
		pick(verts, 0, 1, 2),
	},
	{
    nq().V().HasLabel("Robot"),
		pick(verts, 3, 4, 5),
	},
	{
    nq().V().HasLabel("Robot", "Human"),
		pick(verts, 0, 1, 2, 3, 4, 5),
	},
	{
    nq().V().HasLabel("non-existant"),
    pick(verts),
	},
	{
    nq().V().HasID("v0", "v2"),
		pick(verts, 0, 2),
	},
	{
    nq().V().HasID("non-existant"),
    pick(verts),
	},
	{
    nq().V().Limit(2),
		pick(verts, 0, 1),
	},
	{
    nq().V().Count(),
		[]*traveler{
      {dataType: countData, count: int64(len(verts))},
		},
	},
  {
    nq().V().HasLabel("Human").Has("name", "Ryan"),
    pick(verts, 2),
  },
  {
    nq().V().HasLabel("Human").
      As("x").Has("name", "Alex").Select("x"),
    pick(verts, 0),
  },
  {
    nq().V(),
    verts,
  },
  {
    nq().E(),
    edges,
  },
}

func TestProcs(t *testing.T) {
	for _, desc := range table {
		t.Run(desc.query.String(), func(t *testing.T) {
			// Catch pipes which forget to close their out channel
			// by requiring they process quickly.
			timer := time.NewTimer(time.Millisecond * 5000)
			// "done" is closed when the pipe finishes.
			done := make(chan struct{})

			go func() {
        in := make(chan *traveler)
        out := make(chan *traveler)
        defer close(done)

        // Write an empty traveler to input
        // to trigger the computation.
        go func() {
          in <- &traveler{}
          close(in)
        }()
        q := desc.query.GraphQuery.Query
        procs, err := compile(db, q)
        if err != nil {
          t.Fatal(err)
        }
        go run(procs, in, out, 10)

        // Run processors and collect results
        res := []*traveler{}
        for t := range out {
          res = append(res, t)
        }

				if !timer.Stop() {
					<-timer.C
				}
				if diff := deep.Equal(res, desc.expected); diff != nil {
					t.Error(diff)
				}
			}()

			select {
			case <-done:
			case <-timer.C:
				t.Log("did you forget to close the out channel?")
				t.Fatal("pipe failed to process in time")
			}
		})
	}
}

func pick(src []*traveler, is ...int) []*traveler {
	out := []*traveler{}
	for _, i := range is {
		out = append(out, src[i])
	}
	return out
}
