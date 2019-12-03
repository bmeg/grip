
package tabular

import (
  "log"
  "context"
  "bytes"
  "encoding/binary"
  "github.com/bmeg/grip/kvi"
)


func SetPathID( kv kvi.KVBulkWrite, path string, num uint64 ) {
  pk := PathKey(path)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num)
  kv.Set(pk, b)
}

func GetPathID(kv kvi.KVTransaction, path string) (uint64, error) {
  pk := PathKey(path)
  v, err := kv.Get(pk)
  if err != nil {
    return 0, err
  }
  o, _ := binary.Uvarint(v)
  return o, nil
}

func NewPathID( kv kvi.KVTransaction, path string ) uint64 {
  ok := PathNumKey()
  num := uint64(0)
  if v, err := kv.Get(ok); err == nil {
    num, _ = binary.Uvarint(v)
  }
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num+1)
  kv.Set(ok, b)

  SetPathID(kv, path, num)
  return num
}

func SetIDLine( kv kvi.KVBulkWrite, pathID uint64, id string, line uint64) {
  ik := IDKey(pathID, id)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, line)
  kv.Set(ik, b)
}


func SetLineOffset( kv kvi.KVBulkWrite, pathID uint64, line uint64, offset uint64) {
  lk := LineKey(pathID, line)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, offset)
  kv.Set(lk, b)
}

func GetLineOffset( kv kvi.KVTransaction, pathID uint64, line uint64 ) (uint64, error) {
  lk := LineKey(pathID, line)
  if v, err := kv.Get(lk); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func GetIDLine(kv kvi.KVTransaction, pathID uint64, id string) (uint64, error) {
  ik := IDKey(pathID, id)
  if v, err := kv.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    log.Printf("Line '%s' not found", id)
    return 0, err
  }
}


func GetLineCount(kv kvi.KVTransaction, pathID uint64) (uint64, error) {
  ik := LineCountKey(pathID)
  if v, err := kv.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}


func SetLineCount(kv kvi.KVBulkWrite, pathID, lineCount uint64) {
  lk := LineCountKey(pathID)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, lineCount)
  kv.Set(lk, b)
}


func GetIDChannel(ctx context.Context, kv kvi.KVInterface, pathID uint64) chan string {
  out := make(chan string, 10)
  go func() {
    defer close(out)
    kv.View(func(it kvi.KVIterator) error {
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

func SetColumnIndex(kv kvi.KVBulkWrite, pathID, col uint64, value string, lineCount uint64) {
  idxKey := IndexKey(pathID, col, value, lineCount)
  kv.Set(idxKey, []byte{})
}
