package kvindex

import (
	//"context"
	"github.com/bmeg/arachne/kvgraph"
	"bytes"
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

func FieldTermKeyParse(key []byte) (string, []byte) {
	tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
	term := tmp[2]
	return field, term
}

//key: i | field | term | docid
//val:

var idxFieldTermDocPrefix = []byte("i")


func FieldTermDocKey(field string, term []byte, docid string) []byte {
  return bytes.Join([][]byte{idxFieldTermDocPrefix, []byte(field), term, []byte(docid)}, []byte{0})
}

func FieldTermDocKeyParse(key []byte) (string, []byte, string) {
  tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
  term := tmp[2]
  docid := tmp[3]
	return field, term, string(docid)
}

//key: d | docid
//val:

var idxDocPrefix = []byte("b")

type KVIndex struct {
  kv kvgraph.KVInterface
}


func NewIndex(kv kvgraph.KVInterface) *KVIndex {
	return &KVIndex{kv}
}

func (idx *KVIndex) AddField(name string) error {
	return nil
}

func (idx *KVIndex) RemoveField(name string) error {
	return nil
}

func (idx *KVIndex) AddDocField(field string, docId string, field interface{}) {

}

func (idx *KVIndex) ListFields() chan string {
	out := make(chan string)
	go func() {
		defer close(out)
	}()
	return out
}

func (idx *KVIndex) AddDoc(docId string, value interface{}) error {
	return AddDocPrefix(docId, value, "")
}

func (idx *KVIndex) AddDocPrefix(docId string, value interface{}, fieldPrefix string) error {
	return nil
}

func (idx *KVIndex) UpdateDoc(docId string, value interface{}, fieldPrefix string) error {
	return nil
}

func (idx *KVIndex) RemoveDoc(docId string) error {
	return nil
}
