package engine

import (
	"github.com/bmeg/arachne/aql"
	"github.com/go-test/deep"
	"testing"
	"time"
)

type dat map[string]interface{}

var elements = []*Element{
	{"0", "Human", Vertex, dat{"name": "Alex"}},
	{"1", "Human", Vertex, dat{"name": "Kyle"}},
	{"2", "Human", Vertex, dat{"name": "Adam"}},
	{"3", "Robot", Vertex, dat{"name": "C3P0"}},
	{"4", "Robot", Vertex, dat{"name": "R2D2"}},
	{"5", "Robot", Vertex, dat{"name": "Bender"}},
	{"6", "Clone", Vertex, dat{"name": "Alex"}},
	{"7", "Clone", Vertex, dat{"name": "Kyle"}},
	{"8", "Clone", Vertex, dat{"name": "Adam"}},
}

var table = []struct {
	name string
	pipe
	expected []*Element
}{
	{
		"hasData",
		&hasData{stmt: &aql.HasStatement{
			Key: "name", Within: []string{"Kyle", "Alex"}}},
		pick(0, 1, 6, 7),
	},
	{
		"hasData on non-existant key",
		&hasData{stmt: &aql.HasStatement{
			Key: "never", Within: []string{"Kyle", "Alex"}}},
		pick(),
	},
	{
		"hasLabel",
		&hasLabel{labels: []string{"Human"}},
		pick(0, 1, 2),
	},
	{
		"hasLabel",
		&hasLabel{labels: []string{"Robot"}},
		pick(3, 4, 5),
	},
	{
		"hasLabel with multiple labels",
		&hasLabel{labels: []string{"Robot", "Human"}},
		pick(0, 1, 2, 3, 4, 5),
	},
	{
		"hasLabel with non-existant label",
		&hasLabel{labels: []string{"never"}},
		pick(),
	},
	{
		"hasID",
		&hasID{ids: []string{"0", "2"}},
		pick(0, 2),
	},
	{
		"hasID with non-existant ID",
		&hasID{ids: []string{"never"}},
		pick(),
	},
	{
		"limit",
		&limit{2},
		pick(0, 1),
	},
	{
		"count",
		&count{},
		[]*Element{
			{Type: Count, Data: dat{"count": len(elements)}},
		},
	},
  {
    "chain single",
    &chain{pipes: []pipe{
      &hasData{stmt: &aql.HasStatement{
        Key: "name", Within: []string{"Adam"}}},
    }},
    pick(2, 8),
  },
  {
    "chain two",
    &chain{pipes: []pipe{
      &hasLabel{labels: []string{"Human"}},
      &hasData{stmt: &aql.HasStatement{
        Key: "name", Within: []string{"Adam"}}},
    }},
    pick(2),
  },
}

func TestPipes(t *testing.T) {
	for _, desc := range table {
		t.Run(desc.name, func(t *testing.T) {
			// Catch pipes which forget to close their out channel
			// by requiring they process quickly.
			timer := time.NewTimer(time.Millisecond)
			// "done" is closed when the pipe finishes.
			done := make(chan struct{})

			go func() {
				res := processElements(desc.pipe)
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

func pick(is ...int) []*Element {
	out := []*Element{}
	for _, i := range is {
		out = append(out, elements[i])
	}
	return out
}

func processElements(p pipe) []*Element {
	in := make(chan *Element)
	out := make(chan *Element)
	res := []*Element{}
	go p.Process(in, out)
	go func() {
		for _, el := range elements {
			in <- el
		}
		close(in)
	}()
	for o := range out {
		res = append(res, o)
	}
	return res
}
