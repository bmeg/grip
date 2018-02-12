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

var verts = []*traveler{
  vert("0", "Human", dat{"name": "Alex"}),
  vert("1", "Human", dat{"name": "Kyle"}),
  vert("2", "Human", dat{"name": "Ryan"}),
  vert("3", "Robot", dat{"name": "C-3PO"}),
  vert("4", "Robot", dat{"name": "R2-D2"}),
  vert("5", "Robot", dat{"name": "Bender"}),
  vert("6", "Clone", dat{"name": "Alex"}),
  vert("7", "Clone", dat{"name": "Kyle"}),
  vert("8", "Clone", dat{"name": "Ryan"}),
  vert("9", "Clone", nil),
}

func nq() *aql.Query {
  return aql.NewQuery("test").V()
}

var table = []struct {
  query *aql.Query
	expected []*traveler
}{
	{
    nq().Has("name", "Kyle", "Alex"),
		pick(0, 1, 6, 7),
	},
	{
    nq().Has("non-existant", "Kyle", "Alex"),
		pick(),
	},
	{
    nq().HasLabel("Human"),
		pick(0, 1, 2),
	},
	{
    nq().HasLabel("Robot"),
		pick(3, 4, 5),
	},
	{
    nq().HasLabel("Robot", "Human"),
		pick(0, 1, 2, 3, 4, 5),
	},
	{
    nq().HasLabel("non-existant"),
		pick(),
	},
	{
    nq().HasID("0", "2"),
		pick(0, 2),
	},
	{
    nq().HasID("non-existant"),
		pick(),
	},
	{
    nq().Limit(2),
		pick(0, 1),
	},
	{
    nq().Count(),
		[]*traveler{
      {dataType: countData, count: int64(len(verts))},
		},
	},
  {
    nq().HasLabel("Human").Has("name", "Ryan"),
    pick(2),
  },
  {
    nq().HasLabel("Human").
      As("x").Has("name", "Alex").Select("x"),
    pick(0),
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
        t.Log(desc.query.String())
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

func pick(is ...int) []*traveler {
	out := []*traveler{}
	for _, i := range is {
		out = append(out, verts[i])
	}
	return out
}
