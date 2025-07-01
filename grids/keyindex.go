package grids

import (
	"bytes"
)

var vertexPrefix = []byte(".")
var edgePrefix = []byte("-")
var srcEdgePrefix = []byte("<")
var dstEdgePrefix = []byte(">")

var intSize = 10

// VertexKey generates the key given a vertexId
func VertexKey(id string) []byte {
	return bytes.Join([][]byte{
		vertexPrefix,
		[]byte(id),
	}, []byte{0})
}

func VertexKeyParse(key []byte) (id string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[1])
}

// EdgeKeyPrefix returns the byte array prefix for a particular edge id
func EdgeKeyPrefix(id string) []byte {
	return bytes.Join([][]byte{
		edgePrefix,
		[]byte(id),
		{},
	}, []byte{0})
}

// SrcEdgePrefix returns a byte array prefix for all entries in the source
// edge index a particular vertex (the source vertex)
func SrcEdgePrefix(id string) []byte {
	return bytes.Join([][]byte{
		srcEdgePrefix,
		[]byte(id),
		{},
	}, []byte{0})
}

// DstEdgePrefix returns a byte array prefix for all entries in the dest
// edge index a particular vertex (the dest vertex)
func DstEdgePrefix(id string) []byte {
	return bytes.Join([][]byte{
		dstEdgePrefix,
		[]byte(id),
		{},
	}, []byte{0})
}

// EdgeKey takes the required components of an edge key and returns the byte array
func EdgeKey(id, src, dst, label string) []byte {
	return bytes.Join([][]byte{
		edgePrefix,
		[]byte(id),
		[]byte(label),
		[]byte(src),
		[]byte(dst),
	}, []byte{0})
}

func EdgeKeyParse(key []byte) (eid string, sid string, did string, label string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[1]), string(tmp[3]), string(tmp[4]), string(tmp[2])
}

// SrcEdgeKey creates a src edge index key
func SrcEdgeKey(eid, src, dst, label string, fromLabel []byte) []byte {
	return bytes.Join([][]byte{
		srcEdgePrefix,
		[]byte(src),
		[]byte(dst),
		[]byte(eid),
		[]byte(label),
		fromLabel,
	}, []byte{0})
}

func SrcEdgeKeyParse(key []byte) (eid string, sid string, did string, label string, fromLabel string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[3]), string(tmp[1]), string(tmp[2]), string(tmp[4]), string(tmp[5])
}

// DstEdgeKey creates a dest edge index key
func DstEdgeKey(eid, src, dst, label string, toLabel []byte) []byte {
	return bytes.Join([][]byte{
		dstEdgePrefix,
		[]byte(dst),
		[]byte(src),
		[]byte(eid),
		[]byte(label),
		toLabel,
	}, []byte{0})
}

func DstEdgeKeyParse(key []byte) (eid string, sid string, did string, label string, toLabel string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[3]), string(tmp[2]), string(tmp[1]), string(tmp[4]), string(tmp[5])
}

func DstEdgeKeyPrefix(eid, sid, did, lbl string) []byte {
	return bytes.Join([][]byte{
		srcEdgePrefix,
		[]byte(did),
		[]byte(sid),
		[]byte(eid),
		[]byte(lbl),
	}, []byte{0})
}

func DstEdgeKeyPrefixParse(key []byte) (eid string, sid string, did string, label string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[3]), string(tmp[2]), string(tmp[1]), string(tmp[4])
}

func SrcEdgeKeyPrefix(eid, sid, did, lbl string) []byte {
	return bytes.Join([][]byte{
		srcEdgePrefix,
		[]byte(sid),
		[]byte(did),
		[]byte(eid),
		[]byte(lbl),
	}, []byte{0})
}

func SrcEdgeKeyPrefixParse(key []byte) (eid string, sid string, did string, label string) {
	tmp := bytes.Split(key, []byte{0})
	return string(tmp[3]), string(tmp[1]), string(tmp[2]), string(tmp[4])
}

// VertexListPrefix returns a byte array prefix for all vertices in a graph
func VertexListPrefix() []byte {
	return bytes.Join([][]byte{
		vertexPrefix,
		{},
	}, []byte{0})
}

// EdgeListPrefix returns a byte array prefix for all edges in a graph
func EdgeListPrefix() []byte {
	return bytes.Join([][]byte{
		edgePrefix,
		{},
	}, []byte{0})
}

// SrcEdgeListPrefix returns a byte array prefix for all entries in the source
// edge index for a graph
func SrcEdgeListPrefix() []byte {
	return bytes.Join([][]byte{
		srcEdgePrefix,
		{},
	}, []byte{0})
}

// DstEdgeListPrefix returns a byte array prefix for all entries in the dest
// edge index for a graph
func DstEdgeListPrefix() []byte {
	return bytes.Join([][]byte{
		dstEdgePrefix,
		{},
	}, []byte{0})
}
