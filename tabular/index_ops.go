
package tabular

import (
  "encoding/binary"
  "github.com/bmeg/grip/kvi"
)

func SetPathValue( kv kvi.KVBulkWrite, path string, num uint64 ) {
  pk := PathKey(path)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num)
  kv.Set(pk, b)
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

func GetLineOffset( kv kvi.KVInterface, pathID uint64, line uint64 ) (uint64, error) {
  lk := LineKey(pathID, line)
  if v, err := kv.Get(lk); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}

func GetIDLine(kv kvi.KVInterface, pathID uint64, id string) (uint64, error) {
  ik := IDKey(pathID, id)
  if v, err := kv.Get(ik); err == nil {
    o, _ := binary.Uvarint(v)
    return o, nil
  } else {
    return 0, err
  }
}
