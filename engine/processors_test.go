package engine

import (
	"github.com/bmeg/arachne/aql"
	"github.com/go-test/deep"
	"testing"
	"time"
)

type dat map[string]interface{}

func vert(id, label string, d dat) *traveler {
  return &traveler{id: id, label: label, dataType: vertexData, data: d}
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

var table = []struct {
	name string
  procs []processor
	expected []*traveler
}{
	{
		"hasData",
    []processor{
      &hasData{stmt: &aql.HasStatement{
        Key: "name", Within: []string{"Kyle", "Alex"}}},
    },
		pick(0, 1, 6, 7),
	},
	{
		"hasData on non-existant key",
    []processor{
      &hasData{stmt: &aql.HasStatement{
        Key: "never", Within: []string{"Kyle", "Alex"}}},
    },
		pick(),
	},
	{
		"hasLabel",
    []processor{
		  &hasLabel{labels: []string{"Human"}},
    },
		pick(0, 1, 2),
	},
	{
		"hasLabel",
    []processor{
		  &hasLabel{labels: []string{"Robot"}},
    },
		pick(3, 4, 5),
	},
	{
		"hasLabel with multiple labels",
    []processor{
		  &hasLabel{labels: []string{"Robot", "Human"}},
    },
		pick(0, 1, 2, 3, 4, 5),
	},
	{
		"hasLabel with non-existant label",
    []processor{
		  &hasLabel{labels: []string{"never"}},
    },
		pick(),
	},
	{
		"hasID",
    []processor{
		  &hasID{ids: []string{"0", "2"}},
    },
		pick(0, 2),
	},
	{
		"hasID with non-existant ID",
    []processor{
		  &hasID{ids: []string{"never"}},
    },
		pick(),
	},
	{
		"limit",
    []processor{
		  &limit{2},
    },
		pick(0, 1),
	},
	{
		"count",
    []processor{
		  &count{},
    },
		[]*traveler{
      {dataType: countData, count: int64(len(verts))},
		},
	},
  {
    `V().hasLabel("Human").`,
    []processor{
      &hasLabel{labels: []string{"Human"}},
      &hasData{stmt: &aql.HasStatement{
        Key: "name", Within: []string{"Ryan"}}},
    },
    pick(2),
  },
}

func TestProcs(t *testing.T) {
	for _, desc := range table {
		t.Run(desc.name, func(t *testing.T) {
			// Catch pipes which forget to close their out channel
			// by requiring they process quickly.
			timer := time.NewTimer(time.Millisecond * 5000)
			// "done" is closed when the pipe finishes.
			done := make(chan struct{})

			go func() {
        in := make(chan *traveler)
        out := make(chan *traveler)

        // Write source data to input
        go func() {
          for _, t := range verts {
            in <- t
          }
          close(in)
        }()
        go run(desc.procs, in, out, 10)

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
				close(done)
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
