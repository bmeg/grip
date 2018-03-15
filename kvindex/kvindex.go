package kvindex

import (
	//"context"
	"bytes"
	"fmt"
	"github.com/bmeg/arachne/kvgraph"
	proto "github.com/golang/protobuf/proto"
	//"log"
	"strings"
)

const BufferSize = 1000

//key: f | field
//val:
var idxFieldPrefix = []byte("f")

//key: t | field | term
//val: count
var idxTermPrefix = []byte("t")

//key: i | field | term | docid
//val:
var idxEntryPrefix = []byte("i")

//key: d | docid
//val: Doc
var idxDocPrefix = []byte("d")

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

func TermKey(field string, term []byte) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), term}, []byte{0})
}

func TermPrefix(field string) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), {}}, []byte{0})
}

func TermKeyParse(key []byte) (string, []byte) {
	tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
	term := tmp[2]
	return field, term
}

func EntryKey(field string, term []byte, docid string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), term, []byte(docid)}, []byte{0})
}

func EntryPrefix(field string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {}}, []byte{0})
}

func EntryValuePrefix(field string, term []byte) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), term, {}}, []byte{0})
}

func EntryKeyParse(key []byte) (string, []byte, string) {
	tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
	term := tmp[2]
	docid := tmp[3]
	return field, term, string(docid)
}

func DocKey(docId string) []byte {
	return bytes.Join([][]byte{idxDocPrefix, []byte(docId)}, []byte{0})
}

func contains(c string, s []string) bool {
	for _, i := range s {
		if c == i {
			return true
		}
	}
	return false
}

func containsPrefix(c string, s []string) bool {
	for _, i := range s {
		if strings.HasPrefix(i, c) {
			return true
		}
	}
	return false

}

type KVIndex struct {
	kv kvgraph.KVInterface
}

type KVTermCount struct {
	Value []byte
	Count int64
}

func NewIndex(kv kvgraph.KVInterface) *KVIndex {
	return &KVIndex{kv}
}

func (idx *KVIndex) AddField(path string) error {
	fk := FieldKey(path)
	return idx.kv.Set(fk, []byte{})
}

func (idx *KVIndex) RemoveField(path string) error {
	fk := FieldKey(path)
	fkt := TermPrefix(path)
	ed := EntryPrefix(path)
	idx.kv.DeletePrefix(fkt)
	idx.kv.DeletePrefix(ed)
	return idx.kv.Delete(fk)
}

func (idx *KVIndex) ListFields() []string {
	out := make([]string, 0, 10)
	fPrefix := FieldPrefix()
	idx.kv.View(func(it kvgraph.KVIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field := FieldKeyParse(it.Key())
			out = append(out, field)
		}
		return nil
	})
	return out
}

func (idx *KVIndex) AddDoc(docId string, value map[string]interface{}) error {
	return idx.AddDocPrefix(docId, value, "")
}

type entryValue struct {
	term     []byte
	termKey  []byte
	entryKey []byte
}

func newEntry(docId string, field string, value interface{}) entryValue {
	var term []byte
	if x, ok := value.(string); ok {
		term = []byte(x)
	}
	t := TermKey(field, term)
	ent := EntryKey(field, term, docId)
	return entryValue{term: term, termKey: t, entryKey: ent}
}

func fieldScan(docId string, doc map[string]interface{}, fieldPrefix string, fields []string, out chan entryValue) {
	for k, v := range doc {
		f := fmt.Sprintf("%s.%s", fieldPrefix, k)
		if containsPrefix(f, fields) {
			if x, ok := v.(map[string]interface{}); ok {
				fieldScan(docId, x, fmt.Sprintf("%s.%s", fieldPrefix, k), fields, out)
			} else if contains(f, fields) {
				out <- newEntry(docId, f, v)
			}
		}
	}
}

func (idx *KVIndex) AddDocPrefix(docId string, doc map[string]interface{}, fieldPrefix string) error {
	fields := idx.ListFields()
	values := make(chan entryValue, BufferSize)
	go func() {
		fieldScan(docId, doc, fieldPrefix, fields, values)
		close(values)
	}()
	docKey := DocKey(docId)
	idx.kv.Update(func(tx kvgraph.KVTransaction) error {
		sdoc := Doc{Terms: [][]byte{}}
		for v := range values {
			//log.Printf("Index %#v", v)
			tx.Set(v.entryKey, []byte{})
			tx.Set(v.termKey, []byte{})
			sdoc.Terms = append(sdoc.Terms, v.term)
		}
		data, _ := proto.Marshal(&sdoc)
		tx.Set(docKey, data)
		return nil
	})

	return nil
}

func (idx *KVIndex) RemoveDoc(docId string) error {
	return nil
}

func term2Bytes(term interface{}) []byte {
	if x, ok := term.(string); ok {
		return []byte(x)
	}
	return nil
}

func (idx *KVIndex) GetTermMatch(field string, value interface{}) chan string {
	out := make(chan string, BufferSize)
	go func() {
		term := term2Bytes(value)
		entryPrefix := EntryValuePrefix(field, term)
		defer close(out)
		idx.kv.View(func(it kvgraph.KVIterator) error {
			for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
				_, _, doc := EntryKeyParse(it.Key())
				out <- doc
			}
			return nil
		})
	}()
	return out
}

func (idx *KVIndex) FieldTerms(field string) chan interface{} {
	out := make(chan interface{}, BufferSize)
	go func() {
		termPrefix := TermPrefix(field)
		defer close(out)
		idx.kv.View(func(it kvgraph.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, entry := TermKeyParse(it.Key())
				out <- string(entry)
			}
			return nil
		})
	}()
	return out
}

func (idx *KVIndex) FieldTermCounts(field string) chan KVTermCount {
	terms := make(chan []byte, BufferSize)
	go func() {
		defer close(terms)
		termPrefix := TermPrefix(field)
		idx.kv.View(func(it kvgraph.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, term := TermKeyParse(it.Key())
				terms <- term
			}
			return nil
		})
	}()
	out := make(chan KVTermCount, BufferSize)
	go func() {
		defer close(out)
		for term := range terms {
			entryPrefix := EntryValuePrefix(field, term)
			var count int64
			idx.kv.View(func(it kvgraph.KVIterator) error {
				for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
					count++
				}
				return nil
			})
			out <- KVTermCount{Value: term, Count: count}
		}
	}()
	return out
}
