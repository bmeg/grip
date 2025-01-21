package core

import (
	"context"
	"encoding/json"

	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// Sort rows
type Sort struct {
	sortFields []*gripql.SortField
}

// FromBytes implements logic.SortConf.
func (s *Sort) FromBytes(v []byte) gdbi.Traveler {
	newTraveler := gdbi.BaseTraveler{}
	err := json.Unmarshal(v, &newTraveler)
	if err != nil {
		log.Errorf("sort error: %s", err)
	}
	return &newTraveler
}

// ToBytes implements logic.SortConf.
func (s *Sort) ToBytes(a gdbi.Traveler) []byte {
	v, _ := json.Marshal(a)
	return v
}

func (s *Sort) Compare(a, b gdbi.Traveler) int {
	for _, f := range s.sortFields {
		aVal := gdbi.TravelerPathLookup(a, f.Field)
		bVal := gdbi.TravelerPathLookup(b, f.Field)
		x := logic.CompareAny(aVal, bVal)
		//fmt.Printf("Compare %s v %s = %d\n", aVal, bVal, x)
		if x != 0 {
			if f.Decending {
				return -x
			} else {
				return x
			}
		}
	}
	return 0
}

// Process runs LookupEdges
func (s *Sort) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	signals := []gdbi.Traveler{}

	//sorter := logic.NewMemSorter[gdbi.Traveler](s)

	tmpDir := man.GetTmpDir()
	sorter := logic.NewKVSorter(tmpDir, s)

	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				signals = append(signals, t)
			} else {
				sorter.Add(t)
			}
		}
		//emit signals first (?)
		for _, s := range signals {
			out <- s
		}
		for i := range sorter.Sorted() {
			out <- i
		}
		sorter.Close()
	}()
	return ctx
}
