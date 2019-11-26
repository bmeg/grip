package tabular



import (
	"encoding/binary"
)

var pathCount  = []byte("c")
var pathPrefix = []byte("p")
var linePrefix = []byte("l")
var idPrefix   = []byte("i")
var countPrefix = []byte("n")

func PathNumKey() []byte {
	return pathCount
}

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
func LineKey(pathID, line uint64) []byte {
  out := make([]byte, 1+binary.MaxVarintLen64*2)
	out[0] = linePrefix[0]
	binary.PutUvarint(out[1:binary.MaxVarintLen64+1], pathID)
	binary.PutUvarint(out[binary.MaxVarintLen64+1:2*binary.MaxVarintLen64+1], line)
  return out
}


// IDKey produces the byte key for a particular file path
func IDKey(pathID uint64, id string) []byte {
  p := []byte(id)
  out := make([]byte, 1 + binary.MaxVarintLen64 + len(p))
  out[0] = idPrefix[0]
	binary.PutUvarint(out[1:binary.MaxVarintLen64+1], pathID)
  for i := 0; i < len(p); i++ {
    out[i+1+binary.MaxVarintLen64] = p[i]
  }
  return out
}


func LineCountKey(pathID uint64) []byte {
	out := make([]byte, 1+binary.MaxVarintLen64)
	out[0] = countPrefix[0]
	binary.PutUvarint(out[1:binary.MaxVarintLen64+1], pathID)
  return out
}
