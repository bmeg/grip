
package kvtest


import (
  "bytes"
  "encoding/binary"
  "testing"
  "math/rand"
)

var vertexPrefix = []byte("v")

func vertexStringKey(graph, id string) []byte {
	return bytes.Join([][]byte{vertexPrefix, []byte(graph), []byte(id)}, []byte{0})
}

func vertexStringParse(key []byte) (string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	vid := tmp[2]
	return string(graph), string(vid)
}

func vertexIntKey(graph, key uint64) []byte {
  out := make([]byte, binary.MaxVarintLen64 * 2 + 1)
  out[0] = vertexPrefix[0]
  binary.PutUvarint(out[1:binary.MaxVarintLen64+1], graph)
  binary.PutUvarint(out[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
  return out
}

func vertexIntParse(key []byte) (uint64, uint64) {
  graph, _ := binary.Uvarint(key[1:binary.MaxVarintLen64+1])
  id, _ := binary.Uvarint(key[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1])
  return graph, id
}

var keySetSize = 100000
func BenchmarkKeyCreateString(b *testing.B) {
  keys := make([]string, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := RandStringRunes(20)
    keys = append(keys, s)
  }

  b.Run("vertex-id-create-string", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      vertexStringKey("myGraph", keys[i%keySetSize])
    }
  })
}

func BenchmarkKeyCreateInt(b *testing.B) {
  keys := make([]uint64, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := rand.Uint64()
    keys = append(keys, s)
  }
  b.Run("vertex-id-create-int", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      vertexIntKey(1, keys[i%keySetSize])
    }
  })
}

func BenchmarkKeyParseString(b *testing.B) {
  keys := make([][]byte, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := RandStringRunes(20)
    keys = append(keys, vertexStringKey("myGraph", s))
  }
  b.Run("vertex-id-parse-string", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      vertexStringParse(keys[i%keySetSize])
    }
  })
}

func BenchmarkKeyParseInt(b *testing.B) {
  keys := make([][]byte, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := rand.Uint64()
    keys = append(keys, vertexIntKey(1,s))
  }
  b.Run("vertex-id-parse-int", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      vertexIntParse(keys[i%keySetSize])
    }
  })
}


// Making sure key parsing functions work
func TestKeyParseInt(b *testing.T) {
  for i := 0; i < keySetSize; i++ {
    s := rand.Uint64()
    v := vertexIntKey(1,s)
    g, o := vertexIntParse(v)
    if g != 1 {
      b.Errorf("GraphError")
    }
    if s != o {
      b.Errorf("VertexID Error %#v != %#v", s, o)
    }
  }
}
