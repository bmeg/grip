
package kvtest


import (
  "bytes"
  "encoding/binary"
  "testing"
  "math/rand"
)

var vertexPrefix = []byte("v")
var edgePrefix = []byte("e")

var intSize = 10

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
  out := make([]byte, intSize * 2 + 1)
  out[0] = vertexPrefix[0]
  binary.PutUvarint(out[1:intSize+1], graph)
  binary.PutUvarint(out[intSize+1:intSize*2+1], key)
  return out
}

func vertexIntParse(key []byte) (uint64, uint64) {
  graph, _ := binary.Uvarint(key[1:intSize+1])
  id, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
  return graph, id
}

func edgeStringKey(graph, id, src, dst, label string) []byte {
	return bytes.Join([][]byte{edgePrefix, []byte(graph), []byte(id), []byte(src), []byte(dst), []byte(label)}, []byte{0})
}

func edgeStringKeyParse(key []byte) (string, string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	eid := tmp[2]
	sid := tmp[3]
	did := tmp[4]
	label := tmp[5]
	return string(graph), string(eid), string(sid), string(did), string(label)
}

var edgeSize = 1 + intSize * 5

func edgeIntKey(graph, id, src, dst, label uint64) []byte {
  out := make([]byte, edgeSize)
  out[0] = edgePrefix[0]
  binary.PutUvarint(out[1:intSize+1], graph)
  binary.PutUvarint(out[intSize+1:intSize*2+1], id)
  binary.PutUvarint(out[intSize*2+1:intSize*3+1], src)
  binary.PutUvarint(out[intSize*3+1:intSize*4+1], dst)
  binary.PutUvarint(out[intSize*4+1:intSize*5+1], label)
  return out
}

func edgeIntKeyParse(key []byte) (uint64, uint64, uint64, uint64, uint64) {
	graph, _ := binary.Uvarint(key[1:intSize+1])
	eid, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
	sid, _ := binary.Uvarint(key[intSize*2+1:intSize*3+1])
	did, _ := binary.Uvarint(key[intSize*3+1:intSize*4+1])
	label, _ := binary.Uvarint(key[intSize*4+1:intSize*5+1])
	return graph, eid, sid, did, label
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

type edgeStrings struct {
  id string
  src string
  dst string
  label string
}

type edgeInts struct {
  id uint64
  src uint64
  dst uint64
  label uint64
}

func BenchmarkEdgeStringCreate(b *testing.B) {
  keys := make([]edgeStrings, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := edgeStrings{
      RandStringRunes(10),
      RandStringRunes(10),
      RandStringRunes(10),
      RandStringRunes(8),
    }
    keys = append(keys, s)
  }

  b.Run("edge-id-create-string", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      e := keys[i%keySetSize]
      edgeStringKey("myGraph", e.id, e.src, e.dst, e.label)
    }
  })

}

func BenchmarkEdgeIntCreate(b *testing.B) {
  keys := make([]edgeInts, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    s := edgeInts{
      rand.Uint64(),
      rand.Uint64(),
      rand.Uint64(),
      rand.Uint64(),
    }
    keys = append(keys, s)
  }

  b.Run("edge-id-create-int", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      e := keys[i%keySetSize]
      edgeIntKey(1, e.id, e.src, e.dst, e.label)
    }
  })
}

func BenchmarkEdgeKeyParseString(b *testing.B) {
  keys := make([][]byte, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    e := edgeStrings{
      RandStringRunes(10),
      RandStringRunes(10),
      RandStringRunes(10),
      RandStringRunes(8),
    }
    keys = append(keys, edgeStringKey("myGraph", e.id, e.src, e.dst, e.label))
  }
  b.Run("edge-id-parse-string", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      edgeStringKeyParse(keys[i%keySetSize])
    }
  })
}

func BenchmarkEdgeKeyParseInt(b *testing.B) {
  keys := make([][]byte, 0, keySetSize)
  for i := 0; i < keySetSize; i++ {
    e := edgeInts{
      rand.Uint64(),
      rand.Uint64(),
      rand.Uint64(),
      rand.Uint64(),
    }
    keys = append(keys, edgeIntKey(1, e.id, e.src, e.dst, e.label))
  }
  b.Run("edge-id-parse-int", func(b *testing.B) {
    for i := 0; i < b.N; i++ {
      edgeIntKeyParse(keys[i%keySetSize])
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


func TestEdgeKeyParseInt(b *testing.T) {

  for i := 0; i < keySetSize; i++ {
    iid := rand.Uint64()
    isrc := rand.Uint64()
    idst := rand.Uint64()
    ilabel := rand.Uint64()

    key := edgeIntKey(1, iid, isrc, idst, ilabel)

    dgraph, did, dsrc, ddst, dlabel := edgeIntKeyParse(key)
    if dgraph != 1 {
      b.Errorf("GraphError")
    }
    if iid != did {
      b.Errorf("EdgeID Error %#v != %#v", iid, did)
    }
    if isrc != dsrc {
      b.Errorf("EdgeSrc Error %#v != %#v", isrc, dsrc)
    }
    if idst != ddst {
      b.Errorf("EdgeDST Error %#v != %#v", idst, ddst)
    }
    if ilabel != dlabel {
      b.Errorf("EdgeLabel Error %#v != %#v", ilabel, dlabel)
    }
  }
}
