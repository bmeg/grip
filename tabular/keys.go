package tabular



import (
	"encoding/binary"
)

var pathPrefix = []byte("p")
var linePrefix = []byte("l")
var idPrefix   = []byte("i")


// PathKey produces the byte key for a particular file path
func PathKey(path string) []byte {
  p := []byte(path)
  out := make([]byte, 1 + len(p))
  p[0] = pathPrefix[0]
  for i := 0; i < len(p); i++ {
    out[i+1] = p[i]
  }
  return out
}


// LineKey
func LineKey(line uint64) []byte {
  out := make([]byte, 1+binary.MaxVarintLen64)
	out[0] = linePrefix[0]
	binary.PutUvarint(out[1:binary.MaxVarintLen64+1], line)
  return out
}


// IDKey produces the byte key for a particular file path
func IDKey(id string) []byte {
  p := []byte(id)
  out := make([]byte, 1 + len(p))
  p[0] = idPrefix[0]
  for i := 0; i < len(p); i++ {
    out[i+1] = p[i]
  }
  return out
}
