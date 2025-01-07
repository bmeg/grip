package jobstorage

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/bmeg/grip/gdbi"
)

func TestSerializeStream(t *testing.T) {
	totalCount := 1003
	is := make(chan gdbi.Traveler, 10)
	bs := MarshalStream(is, 4)
	os := UnmarshalStream(bs, 4)

	go func() {
		defer close(is)
		for i := 0; i < totalCount; i++ {
			t := gdbi.BaseTraveler{Current: &gdbi.Vertex{ID: fmt.Sprintf("%d", i)}}
			is <- &t
		}
	}()

	count := 0
	for o := range os {
		if o.GetCurrent() == nil {
			t.Errorf("Incorrect ouput")
		} else {
			if i, err := strconv.Atoi(o.GetCurrent().Get().ID); err == nil {
				if i != count {
					t.Errorf("Incorrect ouput order")
				}
			} else {
				t.Errorf("Incorrect ouput")
			}
		}
		count++
	}

	if count != totalCount {
		t.Errorf("Wrong output count: %d != %d", count, totalCount)
	}
}
