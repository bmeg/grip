
package tabular

import (
  "encoding/binary"
  "github.com/bmeg/grip/kvi"
)

func SetPathValue( kv kvi.KVInterface, path string, num uint64 ) {
  pk := PathKey(path)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, num)
  kv.Set(pk, b)
}


func SetIDLine( kv kvi.KVInterface, pathID uint64, id string, line uint64) {
  ik := IDKey(pathID, id)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, line)
  kv.Set(ik, b)
}


func SetLineOffset( kv kvi.KVInterface, pathID uint64, line uint64, offset uint64) {
  lk := LineKey(pathID, line)
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, offset)
  kv.Set(lk, b)
}

func GetLineOffset( kv kvi.KVInterface, pathID uint64, line uint64 ) uint64 {
  lk := LineKey(pathID, line)
  v, _ := kv.Get(lk)
  o, _ := binary.Uvarint(v)
  return o
}

func GetIDLine(kv kvi.KVInterface, pathID uint64, id string) uint64 {
  ik := IDKey(pathID, id)
  v, _ := kv.Get(ik)
  o, _ := binary.Uvarint(v)
  return o
}
