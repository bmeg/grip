
package kvcache

import (
  "log"
  "fmt"
  "strconv"
  "context"
  "bytes"
  "encoding/binary"
  "github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/tabular"
  "github.com/bmeg/grip/kvindex"
  "github.com/bmeg/grip/kvi/badgerdb"

)

type KVCache struct {
  kvindex.KVIndex
}

type LineIndex struct {
  cache   *KVCache
  pathID  uint64
}

func KVCacheBuilder(path string) (tabular.Cache, error) {
  kv, err := badgerdb.NewKVInterface(path, kvi.Options{})
  if err != nil {
    return nil, err
  }
  return &KVCache{kvindex.KVIndex{KV:kv, Fields:map[string][]string{}}}, nil
}

var loaded = tabular.AddCache("kv", KVCacheBuilder)


func (t *KVCache) GetLineIndex(path string) (tabular.LineIndex, error) {
  pk := PathKey(path)
  v, err := t.KV.Get(pk)
  if err != nil {
    return nil, err
  }
  o, _ := binary.Uvarint(v)
  return &LineIndex{t, o}, nil
}


func (t *KVCache) NewLineIndex( path string ) (tabular.LineIndex, error) {
  ok := PathNumKey()
  num := uint64(0)
  if v, err := t.KV.Get(ok); err == nil {
    num, _ = binary.Uvarint(v)
  }
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num+1)
  t.KV.Set(ok, b) //Make part of same transaction as Get above?
  return &LineIndex{t,num}, nil
}

func (t *KVCache) GetRowStorage(path string) (tabular.RowStorage, error) {
  pk := PathKey(path)
  v, err := t.KV.Get(pk)
  if err != nil {
    return nil, err
  }
  o, _ := binary.Uvarint(v)
  return &KVRowStorage{t, o}, nil
}

func (t *KVCache) NewRowStorage( path string ) (tabular.RowStorage, error) {
  ok := PathNumKey()
  num := uint64(0)
  if v, err := t.KV.Get(ok); err == nil {
    num, _ = binary.Uvarint(v)
  }
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num+1)
  t.KV.Set(ok, b) //Make part of same transaction as Get above?
  return &KVRowStorage{t,num}, nil
}


func (t *LineIndex) AddIndexedField(colName string) {
  colPath := fmt.Sprintf("%d.%s", t.pathID, colName)
  t.cache.AddField(colPath)
}


func (t *LineIndex) GetIDLine(id string) (uint64, error) {
  ik := IDKey(t.pathID, id)
  if v, err := t.cache.KV.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    log.Printf("Line '%s' not found", id)
    return 0, err
  }
}


func (t *LineIndex) GetLineCount() (uint64, error) {
  ik := LineCountKey(t.pathID)
  if v, err := t.cache.KV.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func (t *LineIndex) GetLineOffset(line uint64 ) (uint64, error) {
  lk := LineKey(t.pathID, line)
  if v, err := t.cache.KV.Get(lk); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func (t *LineIndex) GetIDChannel(ctx context.Context) chan string {
  out := make(chan string, 10)
  go func() {
    defer close(out)
    t.cache.KV.View(func(it kvi.KVIterator) error {
      prefix := IDPrefix(t.pathID)
      for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
        select {
  				case <-ctx.Done():
  					return nil
  				default:
  			}
        _, id := IDKeyParse(it.Key())
        out <- string(id)
      }
      return nil
    })
  } ()
  return out
}


func (t *LineIndex) GetLinesByField(ctx context.Context, field string, value string) chan uint64 {
  out := make(chan uint64, 10)
  go func() {
    defer close(out)
    f := fmt.Sprintf("%d.%s", t.pathID, field)
    for i := range t.cache.GetTermMatch(ctx, f, value, -1) {
      o, err := strconv.ParseUint(i, 10, 64)
      if err == nil {
        out <- o
      }
    }
  }()

  return out
}


func (t *LineIndex) IndexWrite( f func(tabular.LineIndexWriter) error ) {
  t.cache.KV.BulkWrite(func(bl kvi.KVBulkWrite) error {
    return f(&IndexWriter{t, bl, t.pathID})
  })
}

type IndexWriter struct {
  parent *LineIndex
  kv kvi.KVBulkWrite
  pathID uint64
}

func (w *IndexWriter) SetIDLine(id string, line uint64) {
  ik := IDKey(w.pathID, id)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, line)
  w.kv.Set(ik, b)
}

func (w *IndexWriter) SetLineOffset(line uint64, offset uint64) {
  lk := LineKey(w.pathID, line)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, offset)
  w.kv.Set(lk, b)
}


func (w *IndexWriter) SetLineCount(lineCount uint64) {
  lk := LineCountKey(w.pathID)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, lineCount)
  w.kv.Set(lk, b)
}

func (w *IndexWriter) IndexRow(line uint64, row map[string]interface{}) error {
  docID := fmt.Sprintf("%d", line)
  pathS := fmt.Sprintf("%d", w.pathID)
  d := map[string]interface{}{
    pathS : row,
  }
  return w.parent.cache.AddDocTx(w.kv, docID, d)
}
