package kvgraph

import (
	"bytes"
)

var graphPrefix = []byte("g")
var vertexPrefix = []byte("v")
var edgePrefix = []byte("e")
var srcEdgePrefix = []byte("s")
var dstEdgePrefix = []byte("d")

var edgeSingle byte = 0x01
var edgeBundle byte = 0x02

// GraphPrefix returns the byte array prefix for all graph entry keys
func GraphPrefix() []byte {
	return graphPrefix
}

// GraphKey produces the byte key for a particular graph
func GraphKey(graph string) []byte {
	return bytes.Join([][]byte{graphPrefix, []byte(graph)}, []byte{0})
}

// GraphKeyParse extracts the string name of a graph from a byte key
func GraphKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	graph := string(tmp[1])
	return graph
}

// EdgeKey takes the required components of an edge key and returns the byte array
func EdgeKey(graph, id, src, dst, label string, etype byte) []byte {
	return bytes.Join([][]byte{edgePrefix, []byte(graph), []byte(id), []byte(src), []byte(dst), []byte(label), {etype}}, []byte{0})
}

// EdgeKeyPrefix returns the byte array prefix for a particular edge id
func EdgeKeyPrefix(graph, id string) []byte {
	return bytes.Join([][]byte{edgePrefix, []byte(graph), []byte(id), {}}, []byte{0})
}

// EdgeKeyParse takes a edge key and returns the elements encoded in it:
// `graph`, `edgeID`, `srcVertexId`, `dstVertexId`, `label`, `edgeType`
func EdgeKeyParse(key []byte) (string, string, string, string, string, byte) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	eid := tmp[2]
	sid := tmp[3]
	did := tmp[4]
	label := tmp[5]
	etype := tmp[6]
	return string(graph), string(eid), string(sid), string(did), string(label), etype[0]
}

// VertexListPrefix returns a byte array prefix for all vertices in a graph
func VertexListPrefix(graph string) []byte {
	return bytes.Join([][]byte{vertexPrefix, []byte(graph)}, []byte{0})
}

// EdgeListPrefix returns a byte array prefix for all edges in a graph
func EdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{edgePrefix, []byte(graph)}, []byte{0})
}

// SrcEdgeKey creates a src edge index key
func SrcEdgeKey(graph, src, dst, eid, label string, etype byte) []byte {
	return bytes.Join([][]byte{srcEdgePrefix, []byte(graph), []byte(src), []byte(dst), []byte(eid), []byte(label), {etype}}, []byte{0})
}

// DstEdgeKey creates a dest edge index key
func DstEdgeKey(graph, src, dst, eid, label string, etype byte) []byte {
	return bytes.Join([][]byte{dstEdgePrefix, []byte(graph), []byte(dst), []byte(src), []byte(eid), []byte(label), {etype}}, []byte{0})
}

// SrcEdgeKeyPrefix creates a byte array prefix for a src edge index entry
func SrcEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{srcEdgePrefix, []byte(graph), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
}

// DstEdgeKeyPrefix creates a byte array prefix for a dest edge index entry
func DstEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{dstEdgePrefix, []byte(graph), []byte(dst), []byte(src), []byte(eid)}, []byte{0})
}

// SrcEdgeKeyParse takes a src index key entry and parses it into:
// `graph`, `srcVertexId`, `dstVertexId`, `edgeId`, `label`, `etype`
func SrcEdgeKeyParse(key []byte) (string, string, string, string, string, byte) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	src := tmp[2]
	dst := tmp[3]
	eid := tmp[4]
	label := tmp[5]
	etype := tmp[6]
	return string(graph), string(src), string(dst), string(eid), string(label), etype[0]
}

// DstEdgeKeyParse takes a dest index key entry and parses it into:
// `graph`, `dstVertexId`, `srcVertexId`, `edgeId`, `label`, `etype`
func DstEdgeKeyParse(key []byte) (string, string, string, string, string, byte) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	dst := tmp[2]
	src := tmp[3]
	eid := tmp[4]
	label := tmp[5]
	etype := tmp[6]
	return string(graph), string(src), string(dst), string(eid), string(label), etype[0]
}

// SrcEdgeListPrefix returns a byte array prefix for all entries in the source
// edge index for a graph
func SrcEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{srcEdgePrefix, []byte(graph), {}}, []byte{0})
}

// DstEdgeListPrefix returns a byte array prefix for all entries in the dest
// edge index for a graph
func DstEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{dstEdgePrefix, []byte(graph), {}}, []byte{0})
}

// SrcEdgePrefix returns a byte array prefix for all entries in the source
// edge index a particular vertex (the source vertex)
func SrcEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{srcEdgePrefix, []byte(graph), []byte(id), {}}, []byte{0})
}

// DstEdgePrefix returns a byte array prefix for all entries in the dest
// edge index a particular vertex (the dest vertex)
func DstEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{dstEdgePrefix, []byte(graph), []byte(id), {}}, []byte{0})
}

// VertexKey generates the key given a vertexId
func VertexKey(graph, id string) []byte {
	return bytes.Join([][]byte{vertexPrefix, []byte(graph), []byte(id)}, []byte{0})
}

// VertexKeyParse takes a byte array key and returns:
// `graphId`, `vertexId`
func VertexKeyParse(key []byte) (string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	vid := tmp[2]
	return string(graph), string(vid)
}
