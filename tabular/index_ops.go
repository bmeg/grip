
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

}


func SetLineOffset( kv kvi.KVInterface, pathID uint64, line uint64, offset uint64) {
  
}
