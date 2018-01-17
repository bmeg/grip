package kvgraph

import (
	"bytes"
)

var GRAPH_PREFIX []byte = []byte("g")
var VERTEX_PREFIX []byte = []byte("v")
var EDGE_PREFIX []byte = []byte("e")
var SEDGE_PREFIX []byte = []byte("s")
var DEDGE_PREFIX []byte = []byte("d")

var EDGE_SINGLE byte = 0x01
var EDGE_BUNDLE byte = 0x02

func GraphPrefix() []byte {
	return GRAPH_PREFIX
}

func GraphKey(graph string) []byte {
	return bytes.Join([][]byte{GRAPH_PREFIX, []byte(graph)}, []byte{0})
}

func GraphKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	graph := string(tmp[1])
	return graph
}

func EdgeKey(graph, id, src, dst, label string, etype byte) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph), []byte(id), []byte(src), []byte(dst), []byte(label), []byte{etype}}, []byte{0})
}

func EdgeKeyPrefix(graph, id string) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

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

func VertexListPrefix(graph string) []byte {
	return bytes.Join([][]byte{VERTEX_PREFIX, []byte(graph)}, []byte{0})
}

func EdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{EDGE_PREFIX, []byte(graph)}, []byte{0})
}

func SrcEdgeKey(graph, src, dst, eid, label string, etype byte) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph), []byte(src), []byte(dst), []byte(eid), []byte(label), []byte{etype}}, []byte{0})
}

func DstEdgeKey(graph, src, dst, eid, label string, etype byte) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph), []byte(dst), []byte(src), []byte(eid), []byte(label), []byte{etype}}, []byte{0})
}

func SrcEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
}

func DstEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph), []byte(dst), []byte(src), []byte(eid)}, []byte{0})
}

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

func SrcEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph)}, []byte{0})
}

func DstEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph)}, []byte{0})
}

func SrcEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{SEDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func DstEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{DEDGE_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func VertexKey(graph, id string) []byte {
	return bytes.Join([][]byte{VERTEX_PREFIX, []byte(graph), []byte(id)}, []byte{0})
}

func VertexKeyParse(key []byte) (string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	vid := tmp[2]
	return string(graph), string(vid)
}
