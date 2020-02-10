package kvcache

import (
  "log"
  "context"
  "bytes"
  "encoding/json"
  "github.com/bmeg/grip/tabular"
  "github.com/bmeg/grip/kvi"
)

type KVRowStorage struct {
  cache   *KVCache
  pathID  uint64
}


func (r *KVRowStorage) Write(row *tabular.TableRow) error {
  key := RowKey(r.pathID, row.Key)
  buf := bytes.Buffer{}
  enc := json.NewEncoder(&buf)
  if err := enc.Encode(row.Values); err != nil {
    log.Printf("Encode error: %s", err)
  }
  return r.cache.KV.Set(key, buf.Bytes())
}


func (r *KVRowStorage) GetRowsByField(ctx context.Context, field string, value string) chan *tabular.TableRow {
  out := make(chan *tabular.TableRow, 10)
  go func() {
    defer close(out)
    prefix := RowPrefix(r.pathID)
    r.cache.KV.View(func(it kvi.KVIterator) error {
      for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
        select {
          case <-ctx.Done():
            return nil
          default:
        }
        _, id := RowKeyParse(it.Key())
        data := map[string]interface{}{}
        v, _ := it.Value()
        buf := bytes.NewBuffer(v)
        dec := json.NewDecoder(buf)
        if err := dec.Decode(&data); err != nil {
          log.Printf("Decode Error: %s", err)
        }
        if tabular.FieldFilter(field, value, data) {
          out <- &tabular.TableRow{Key:string(id), Values:data}
        }
      }
      return nil
    })
  }()
  return out
}
