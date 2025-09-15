package core

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvi"
)

// Group
type Group struct {
	grouping map[string]string
}

func (r *Group) reduce(curTraveler *gdbi.BaseTraveler, newTraveler *gdbi.BaseTraveler) {
	for dest, field := range r.grouping {
		v := gdbi.TravelerPathLookup(newTraveler, field)
		if curTraveler.Current != nil {
			if a, ok := curTraveler.Current.Data[dest]; ok {
				if aSlice, ok := a.([]any); ok {
					curTraveler.Current.Data[dest] = append(aSlice, v)
				} else if !ok {
					// overwrite existing data
					curTraveler.Current.Data[dest] = []any{v}
				}
			} else {
				curTraveler.Current.Data[dest] = []any{v}
			}
		}
	}
}

// Process runs the render processor
func (r *Group) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		kv := man.GetTempKV()
		defer kv.Close()

		//collect
		kv.BulkWrite(func(bl kvi.KVBulkWrite) error {
			var idx uint64 = 0
			idxKey := make([]byte, 8)
			for t := range in {
				if t.IsSignal() {
					out <- t
					continue
				} else {
					//fmt.Printf("Checking %#v\n", t.GetCurrent())
					idStr := t.GetCurrentID()
					binary.LittleEndian.PutUint64(idxKey, idx)
					key := append([]byte(idStr), idxKey...) // the key is the graph element key, plus the index
					if v, err := json.Marshal(t); err == nil {
						bl.Set(key, v)
					}
					idx++
				}
			}
			return nil
		})
		//group
		kv.View(func(it kvi.KVIterator) error {
			it.Seek([]byte{0})
			lastKey := ""
			var curTraveler *gdbi.BaseTraveler
			for it.Seek([]byte{0}); it.Valid(); it.Next() {
				k := it.Key()
				curKey := string(k[0 : len(k)-8]) // remove index suffix
				newTraveler := gdbi.BaseTraveler{}
				value, _ := it.Value()
				json.Unmarshal(value, &newTraveler)
				//fmt.Printf("curKey: (%s) %s\n", curKey, newTraveler)
				if lastKey != curKey {
					if curTraveler != nil {
						out <- curTraveler
					}
					curTraveler = &newTraveler
					lastKey = curKey
					r.reduce(curTraveler, &newTraveler)
				} else {
					r.reduce(curTraveler, &newTraveler)
				}
			}
			if lastKey != "" {
				out <- curTraveler
			}
			return nil
		})

	}()
	return ctx
}
