package key

import (
	"bytes"
	"encoding/binary"

	"github.com/bmeg/benchtop"
)

const (
	VertexTablePrefix = "v_"
	EdgeTablePrefix   = "e_"
)

var vertexPrefix = []byte(".")
var edgePrefix = []byte("-")
var srcEdgePrefix = []byte("<")
var dstEdgePrefix = []byte(">")

// ID mapping prefixes (local to grids or using benchtop ones)
func StringToIDKey(s string) []byte {
	return append([]byte{benchtop.IDMappingPrefix}, []byte(s)...)
}

func IDToStringKey(id uint64) []byte {
	out := make([]byte, 9)
	out[0] = benchtop.RIDMappingPrefix
	binary.BigEndian.PutUint64(out[1:], id)
	return out
}
func ParseRIDKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[1:])
}

// VertexKey generates the key given a vertex uint64 ID
func VertexKey(id uint64) []byte {
	out := make([]byte, 9)
	out[0] = vertexPrefix[0]
	binary.BigEndian.PutUint64(out[1:], id)
	return out
}

func VertexKeyParse(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[1:])
}

// VertexIntegratedParse parses both vertex ID from key and label/loc from value
func VertexIntegratedParse(k, v []byte) (id uint64, label string, locBytes []byte) {
	id = VertexKeyParse(k)
	idx := bytes.IndexByte(v, 0)
	if idx < 0 {
		return id, string(v), nil
	}
	label = string(v[:idx])
	locBytes = v[idx+1:]
	return id, label, locBytes
}

// IntegratedEdgeValueParse extracts RowLoc bytes from an edge index value
func IntegratedEdgeValueParse(v []byte) []byte {
	if len(v) >= 12 {
		return v
	}
	return nil
}

// EdgeKeyPrefix returns the byte array prefix for a particular edge id (uint64)
func EdgeKeyPrefix(id uint64) []byte {
	out := make([]byte, 9)
	out[0] = edgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], id)
	return out
}

// SrcEdgePrefix returns a byte array prefix for all entries in the source
// edge index a particular vertex (the source vertex)
func SrcEdgePrefix(id uint64) []byte {
	out := make([]byte, 9)
	out[0] = srcEdgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], id)
	return out
}

// DstEdgePrefix returns a byte array prefix for all entries in the dest
// edge index a particular vertex (the dest vertex)
func DstEdgePrefix(id uint64) []byte {
	out := make([]byte, 9)
	out[0] = dstEdgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], id)
	return out
}

// EdgeKey takes the required components of an edge key and returns the byte array
func EdgeKey(id, src, dst uint64, label string) []byte {
	// Format: E | id(8) | src(8) | dst(8) | label(var)
	out := make([]byte, 1+8+8+8+len(label))
	out[0] = edgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], id)
	binary.BigEndian.PutUint64(out[9:], src)
	binary.BigEndian.PutUint64(out[17:], dst)
	copy(out[25:], label)
	return out
}

func EdgeKeyParse(key []byte) (eid uint64, sid uint64, did uint64, label string) {
	eid = binary.BigEndian.Uint64(key[1:9])
	sid = binary.BigEndian.Uint64(key[9:17])
	did = binary.BigEndian.Uint64(key[17:25])
	label = string(key[25:])
	return
}

// SrcEdgeKey creates a src edge index key
func SrcEdgeKey(eid, src, dst uint64, label string) []byte {
	// Format: < | src(8) | dst(8) | id(8) | label(var)
	out := make([]byte, 1+8+8+8+len(label))
	out[0] = srcEdgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], src)
	binary.BigEndian.PutUint64(out[9:], dst)
	binary.BigEndian.PutUint64(out[17:], eid)
	copy(out[25:], label)
	return out
}

func SrcEdgeKeyParse(key []byte) (eid uint64, sid uint64, did uint64, label string) {
	sid = binary.BigEndian.Uint64(key[1:9])
	did = binary.BigEndian.Uint64(key[9:17])
	eid = binary.BigEndian.Uint64(key[17:25])
	label = string(key[25:])
	return
}

// DstEdgeKey creates a dest edge index key
func DstEdgeKey(eid, src, dst uint64, label string) []byte {
	// Format: > | dst(8) | src(8) | id(8) | label(var)
	out := make([]byte, 1+8+8+8+len(label))
	out[0] = dstEdgePrefix[0]
	binary.BigEndian.PutUint64(out[1:], dst)
	binary.BigEndian.PutUint64(out[9:], src)
	binary.BigEndian.PutUint64(out[17:], eid)
	copy(out[25:], label)
	return out
}

func DstEdgeKeyParse(key []byte) (eid uint64, sid uint64, did uint64, label string) {
	did = binary.BigEndian.Uint64(key[1:9])
	sid = binary.BigEndian.Uint64(key[9:17])
	eid = binary.BigEndian.Uint64(key[17:25])
	label = string(key[25:])
	return
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
