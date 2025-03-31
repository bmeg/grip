package core

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"
)

// Pivot take an ID, field and value triple an turns it into a merged element
type Pivot struct {
	Stmt *gripql.PivotStep
}

// Process runs the pivot processor
func (r *Pivot) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		kv := man.GetTempKV()
		defer kv.Close()
		kv.BulkWrite(func(bl kvi.KVBulkWrite) error {
			for t := range in {
				if t.IsSignal() {
					out <- t
					continue
				}
				log.Debugf("Checking %#v\n", t.GetCurrent())
				id := gdbi.TravelerPathLookup(t, r.Stmt.Id)
				if idStr, ok := id.(string); ok {
					field := gdbi.TravelerPathLookup(t, r.Stmt.Field)
					if fieldStr, ok := field.(string); ok {
						value := gdbi.TravelerPathLookup(t, r.Stmt.Value)
						if v, err := json.Marshal(value); err == nil {
							key := bytes.Join([][]byte{[]byte(idStr), []byte(fieldStr)}, []byte{0})
							bl.Set(key, v)
						}
					}
				}
			}
			return nil
		})
		kv.View(func(it kvi.KVIterator) error {
			it.Seek([]byte{0})
			lastKey := ""
			curDict := map[string]any{}
			for it.Seek([]byte{0}); it.Valid(); it.Next() {
				tmp := bytes.Split(it.Key(), []byte{0})
				curKey := string(tmp[0])
				curField := string(tmp[1])
				log.Debugln("CURKEY: ", curKey, "CURFIELD: ", curField)
				if lastKey == "" {
					lastKey = curKey
				}
				var curData any
				value, _ := it.Value()
				json.Unmarshal(value, &curData)
				if lastKey != curKey {
					curDict["_id"] = curKey
					out <- &gdbi.BaseTraveler{Render: curDict}
					curDict = map[string]any{}
					curDict[curField] = curData
					lastKey = curKey
				} else {
					curDict[curField] = curData
				}
			}
			if lastKey != "" {
				curDict["_id"] = lastKey
				out <- &gdbi.BaseTraveler{Render: curDict}
			}
			return nil
		})
	}()
	return ctx
}
