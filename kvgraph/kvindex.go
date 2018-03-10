package kvgraph

import (
	"context"
)

//key: f | field
//val:
var idxFieldPrefix = []byte("f")


// GraphPrefix returns the byte array prefix for all graph entry keys
func FieldPrefix() []byte {
	return idxFieldPrefix
}

// GraphKey produces the byte key for a particular graph
func FieldKey(field string) []byte {
	return bytes.Join([][]byte{idxFieldPrefix, []byte(field)}, []byte{0})
}

// GraphKeyParse extracts the string name of a graph from a byte key
func FieldKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	field := string(tmp[1])
	return field
}

//key: t | field | term
//val: count
var idxTermPrefix = []byte("f")

func FieldTermKey(field string, term []byte) []byte {
  return bytes.Join([][]byte{idxTermPrefix, []byte(field), term}, []byte{0})
}

func FieldTermKeyParse(key []byte) (string, []byte)  {
  tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
  term := tmp[2]
	return field, term
}

//key: i | field | term | docid
//val:

var idxTermDocPrefix = []byte("i")

func FieldTermKey(field string, term []byte, docid string) []byte {
  return bytes.Join([][]byte{idxTermDocPrefix, []byte(field), term, []byte(docid)}, []byte{0})
}

func FieldTermKeyParse(key []byte) (string, []byte, string) {
  tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
  term := tmp[2]
  docid := tmp[3]
	return field, term, docid
}

//key: d | docid
//val:

var idxTermDocPrefix = []byte("b")


type KVIndex struct {
  kv KVInterface
}



func (idx *KVIndex) AddField(name string) error {

}


func (idx *KVIndex) AddDocField(field string, docId string, field interface{} ) {


}
