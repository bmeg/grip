package kvcache

import (
	"bytes"
	"context"
	"encoding/json"
	"log"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/multi"
)

type KVRowStorage struct {
	cache  *KVCache
	pathID uint64
}

func (r *KVRowStorage) Write(row *multi.TableRow) error {
	key := RowKey(r.pathID, row.Key)
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(row.Values); err != nil {
		log.Printf("Encode error: %s", err)
	}
	return r.cache.KV.Set(key, buf.Bytes())
}

func (r *KVRowStorage) GetRowByID(id string) (*multi.TableRow, error) {
	key := RowKey(r.pathID, id)
	value, err := r.cache.KV.Get(key)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{}
	buf := bytes.NewBuffer(value)
	dec := json.NewDecoder(buf)
	if err := dec.Decode(&data); err != nil {
		log.Printf("Decode Error: %s", err)
	}
	return &multi.TableRow{Key: string(id), Values: data}, nil
}

func (r *KVRowStorage) GetRowsByField(ctx context.Context, field string, value string) chan *multi.TableRow {
	out := make(chan *multi.TableRow, 10)
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
				if multi.FieldFilter(field, value, data) {
					out <- &multi.TableRow{Key: string(id), Values: data}
				}
			}
			return nil
		})
	}()
	return out
}
