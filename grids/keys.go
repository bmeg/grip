package grids

import (
	"encoding/binary"
)

var graphPrefix = []byte("g")
var vertexPrefix = []byte("v")
var edgePrefix = []byte("e")
var srcEdgePrefix = []byte("s")
var dstEdgePrefix = []byte("d")

var intSize = 10

// GraphPrefix returns the byte array prefix for all graph entry keys
func GraphPrefix() []byte {
	return graphPrefix
}

// GraphKey produces the byte key for a particular graph
func GraphKey(graph uint64) []byte {
	out := make([]byte, intSize * 2 + 1)
  out[0] = graphPrefix[0]
  binary.PutUvarint(out[1:intSize+1], graph)
	return out
}

// GraphKeyParse extracts the string name of a graph from a byte key
func GraphKeyParse(key []byte) uint64 {
	graph, _ := binary.Uvarint(key[1:intSize+1])
	return graph
}

// VertexKey generates the key given a vertexId
func VertexKey(graph, id uint64) []byte {
	out := make([]byte, intSize * 2 + 1)
  out[0] = vertexPrefix[0]
  binary.PutUvarint(out[1:intSize+1], graph)
  binary.PutUvarint(out[intSize+1:intSize*2+1], id)
  return out
}

// VertexKeyParse takes a byte array key and returns:
// `graphId`, `vertexId`
func VertexKeyParse(key []byte) (uint64, uint64) {
	graph, _ := binary.Uvarint(key[1:intSize+1])
  id, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
  return graph, id
}


// EdgeKey takes the required components of an edge key and returns the byte array
func EdgeKey(graph, id, src, dst, label uint64) []byte {
	out := make([]byte, 1 + intSize * 5)
  out[0] = edgePrefix[0]
  binary.PutUvarint(out[1:intSize+1], graph)
  binary.PutUvarint(out[intSize+1:intSize*2+1], id)
  binary.PutUvarint(out[intSize*2+1:intSize*3+1], src)
  binary.PutUvarint(out[intSize*3+1:intSize*4+1], dst)
  binary.PutUvarint(out[intSize*4+1:intSize*5+1], label)
  return out
}

// EdgeKeyPrefix returns the byte array prefix for a particular edge id
func EdgeKeyPrefix(graph, id uint64) []byte {
	out := make([]byte, 1 + intSize * 2)
	out[0] = edgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], id)
	return out
}

// EdgeKeyParse takes a edge key and returns the elements encoded in it:
// `graph`, `edgeID`, `srcVertexId`, `dstVertexId`, `label`
func EdgeKeyParse(key []byte) (uint64, uint64, uint64, uint64, uint64) {
	graph, _ := binary.Uvarint(key[1:intSize+1])
	eid, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
	sid, _ := binary.Uvarint(key[intSize*2+1:intSize*3+1])
	did, _ := binary.Uvarint(key[intSize*3+1:intSize*4+1])
	label, _ := binary.Uvarint(key[intSize*4+1:intSize*5+1])
	return graph, eid, sid, did, label
}

// VertexListPrefix returns a byte array prefix for all vertices in a graph
func VertexListPrefix(graph uint64) []byte {
	out := make([]byte, 1 + intSize)
	out[0] = vertexPrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	return out
}

// EdgeListPrefix returns a byte array prefix for all edges in a graph
func EdgeListPrefix(graph uint64) []byte {
	out := make([]byte, 1 + intSize)
	out[0] = edgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	return out
}

// SrcEdgeListPrefix returns a byte array prefix for all entries in the source
// edge index for a graph
func SrcEdgeListPrefix(graph uint64) []byte {
	out := make([]byte, 1 + intSize)
	out[0] = srcEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	return out
}

// DstEdgeListPrefix returns a byte array prefix for all entries in the dest
// edge index for a graph
func DstEdgeListPrefix(graph uint64) []byte {
	out := make([]byte, 1 + intSize)
	out[0] = dstEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	return out
}


// SrcEdgeKey creates a src edge index key
func SrcEdgeKey(graph, eid, src, dst, label uint64) []byte {
	out := make([]byte,  1 + intSize * 5)
	out[0] = srcEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], src)
	binary.PutUvarint(out[intSize*2+1:intSize*3+1], dst)
	binary.PutUvarint(out[intSize*3+1:intSize*4+1], eid)
	binary.PutUvarint(out[intSize*4+1:intSize*5+1], label)
	return out
}

// DstEdgeKey creates a dest edge index key
func DstEdgeKey(graph, eid, src, dst, label uint64) []byte {
	out := make([]byte,  1 + intSize * 5)
	out[0] = dstEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], dst)
	binary.PutUvarint(out[intSize*2+1:intSize*3+1], src)
	binary.PutUvarint(out[intSize*3+1:intSize*4+1], eid)
	binary.PutUvarint(out[intSize*4+1:intSize*5+1], label)
	return out
}

// SrcEdgeKeyParse takes a src index key entry and parses it into:
// `graph`, `edgeId`, `srcVertexId`, `dstVertexId`, `label`, `etype`
func SrcEdgeKeyParse(key []byte) (uint64, uint64, uint64, uint64, uint64) {
	graph, _ := binary.Uvarint(key[1:intSize+1])
	sid, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
	did, _ := binary.Uvarint(key[intSize*2+1:intSize*3+1])
	eid, _ := binary.Uvarint(key[intSize*3+1:intSize*4+1])
	label, _ := binary.Uvarint(key[intSize*4+1:intSize*5+1])
	return graph, eid, sid, did, label
}

// DstEdgeKeyParse takes a dest index key entry and parses it into:
// `graph`, `edgeId`, `dstVertexId`, `srcVertexId`, `label`, `etype`
func DstEdgeKeyParse(key []byte) (uint64, uint64, uint64, uint64, uint64) {
	graph, _ := binary.Uvarint(key[1:intSize+1])
	did, _ := binary.Uvarint(key[intSize+1:intSize*2+1])
	sid, _ := binary.Uvarint(key[intSize*2+1:intSize*3+1])
	eid, _ := binary.Uvarint(key[intSize*3+1:intSize*4+1])
	label, _ := binary.Uvarint(key[intSize*4+1:intSize*5+1])
	return graph, eid, sid, did, label
}


// SrcEdgeKeyPrefix creates a byte array prefix for a src edge index entry
func SrcEdgeKeyPrefix(graph, eid, src, dst uint64) []byte {
	out := make([]byte,  1 + intSize * 4)
	out[0] = srcEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], src)
	binary.PutUvarint(out[intSize*2+1:intSize*3+1], dst)
	binary.PutUvarint(out[intSize*3+1:intSize*4+1], eid)
	return out
}

// DstEdgeKeyPrefix creates a byte array prefix for a dest edge index entry
func DstEdgeKeyPrefix(graph, src, dst, eid uint64) []byte {
	out := make([]byte,  1 + intSize * 4)
	out[0] = dstEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], dst)
	binary.PutUvarint(out[intSize*2+1:intSize*3+1], src)
	binary.PutUvarint(out[intSize*3+1:intSize*4+1], eid)
	return out
}

// SrcEdgePrefix returns a byte array prefix for all entries in the source
// edge index a particular vertex (the source vertex)
func SrcEdgePrefix(graph, id uint64) []byte {
	out := make([]byte,  1 + intSize * 2)
	out[0] = srcEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], id)
	return out
}

// DstEdgePrefix returns a byte array prefix for all entries in the dest
// edge index a particular vertex (the dest vertex)
func DstEdgePrefix(graph, id uint64) []byte {
	out := make([]byte,  1 + intSize * 2)
	out[0] = dstEdgePrefix[0]
	binary.PutUvarint(out[1:intSize+1], graph)
	binary.PutUvarint(out[intSize+1:intSize*2+1], id)
	return out
}
