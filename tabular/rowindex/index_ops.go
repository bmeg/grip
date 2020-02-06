
package rowindex

import (
  "log"
  "fmt"
  "context"
  "bytes"
  "encoding/binary"
  "github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/kvindex"
)

type TableIndex struct {
  kvindex.KVIndex
}

func NewTableIndex(kv kvi.KVInterface) *TableIndex {
  return &TableIndex{kvindex.KVIndex{KV:kv, Fields:map[string][]string{}}}
}

func (t *TableIndex) SetPathID(path string, num uint64 ) {
  pk := PathKey(path)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num)
  t.KV.Set(pk, b)
}

func (t *TableIndex) GetPathID(path string) (uint64, error) {
  pk := PathKey(path)
  v, err := t.KV.Get(pk)
  if err != nil {
    return 0, err
  }
  o, _ := binary.Uvarint(v)
  return o, nil
}

func (t *TableIndex) NewPathID( path string ) uint64 {
  ok := PathNumKey()
  num := uint64(0)
  if v, err := t.KV.Get(ok); err == nil {
    num, _ = binary.Uvarint(v)
  }
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num+1)
  t.KV.Set(ok, b) //Make part of same transaction as Get above?
  t.SetPathID(path, num)
  return num
}

func (t *TableIndex) GetIDLine(pathID uint64, id string) (uint64, error) {
  ik := IDKey(pathID, id)
  if v, err := t.KV.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    log.Printf("Line '%s' not found", id)
    return 0, err
  }
}


func (t *TableIndex) GetLineCount(pathID uint64) (uint64, error) {
  ik := LineCountKey(pathID)
  if v, err := t.KV.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func (t *TableIndex) GetLineOffset(pathID uint64, line uint64 ) (uint64, error) {
  lk := LineKey(pathID, line)
  if v, err := t.KV.Get(lk); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func (t *TableIndex) GetIDChannel(ctx context.Context, pathID uint64) chan string {
  out := make(chan string, 10)
  go func() {
    defer close(out)
    t.KV.View(func(it kvi.KVIterator) error {
      prefix := IDPrefix(pathID)
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

func (t *TableIndex) IndexWrite( f func(*IndexWriter) error ) {
  t.KV.BulkWrite(func(bl kvi.KVBulkWrite) error {
    return f(&IndexWriter{t, bl})
  })
}

type IndexWriter struct {
  parent *TableIndex
  kv kvi.KVBulkWrite
}

func (w *IndexWriter) SetIDLine( pathID uint64, id string, line uint64) {
  ik := IDKey(pathID, id)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, line)
  w.kv.Set(ik, b)
}

func (w *IndexWriter) SetLineOffset( pathID uint64, line uint64, offset uint64) {
  lk := LineKey(pathID, line)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, offset)
  w.kv.Set(lk, b)
}


func (w *IndexWriter) SetLineCount( pathID, lineCount uint64) {
  lk := LineCountKey(pathID)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, lineCount)
  w.kv.Set(lk, b)
}

func (w *IndexWriter) IndexRow( pathID, line uint64, row map[string]interface{}) error {
  docID := fmt.Sprintf("%d.%d", pathID, line)
  log.Printf("Indexing %s", row)
  return w.parent.AddDocTx(w.kv, docID, row)
}
