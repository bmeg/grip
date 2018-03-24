package kvindex

import (
	//"context"
	"bytes"
	"fmt"
	"github.com/bmeg/arachne/kvi"
	proto "github.com/golang/protobuf/proto"
	"log"
	"strings"
)

const bufferSize = 1000

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

// FieldPrefix returns the byte array prefix for all graph entry keys
func FieldPrefix() []byte {
	return idxFieldPrefix
}

// FieldKey produces the byte key for a particular graph
func FieldKey(field string) []byte {
	return bytes.Join([][]byte{idxFieldPrefix, []byte(field)}, []byte{0})
}

// FieldKeyParse extracts the string name of a graph from a byte key
func FieldKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	field := string(tmp[1])
	return field
}

// TermKey create a key for a term index
func TermKey(field string, term []byte) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), term}, []byte{0})
}

// TermPrefix get a prefix for all the terms for a single field
func TermPrefix(field string) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), {}}, []byte{0})
}

// TermKeyParse parse a term key into a field and a value
func TermKeyParse(key []byte) (string, []byte) {
	tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
	term := tmp[2]
	return field, term
}

// EntryKey create a key for an entry
func EntryKey(field string, term []byte, docid string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), term, []byte(docid)}, []byte{0})
}

// EntryPrefix get prefix for all entries for a single field
func EntryPrefix(field string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {}}, []byte{0})
}

// EntryValuePrefix get prefix for all terms for a field
func EntryValuePrefix(field string, term []byte) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), term, {}}, []byte{0})
}

// EntryKeyParse take entry key and parse out field term and document id
func EntryKeyParse(key []byte) (string, []byte, string) {
	tmp := bytes.Split(key, []byte{0}) //BUG: term may have 0x00 in it
	field := string(tmp[1])
	term := tmp[2]
	docid := tmp[3]
	return field, term, string(docid)
}

// DocKey create a document entry key
func DocKey(docID string) []byte {
	return bytes.Join([][]byte{idxDocPrefix, []byte(docID)}, []byte{0})
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

// KVIndex is a index implementation using the generic key value store
type KVIndex struct {
	kv kvi.KVInterface
}

// KVTermCount Get all terms and their counts
type KVTermCount struct {
	Value []byte
	Count int64
}

// NewIndex create new key value index
func NewIndex(kv kvi.KVInterface) *KVIndex {
	return &KVIndex{kv}
}

// AddField add new field to be indexed
func (idx *KVIndex) AddField(path string) error {
	fk := FieldKey(path)
	return idx.kv.Set(fk, []byte{})
}

// RemoveField removes an indexed field
func (idx *KVIndex) RemoveField(path string) error {
	fk := FieldKey(path)
	fkt := TermPrefix(path)
	ed := EntryPrefix(path)
	idx.kv.DeletePrefix(fkt)
	idx.kv.DeletePrefix(ed)
	return idx.kv.Delete(fk)
}

// ListFields lists all indexed fields
func (idx *KVIndex) ListFields() []string {
	out := make([]string, 0, 10)
	fPrefix := FieldPrefix()
	idx.kv.View(func(it kvi.KVIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field := FieldKeyParse(it.Key())
			out = append(out, field)
		}
		return nil
	})
	return out
}

// AddDoc adds new document to the index
func (idx *KVIndex) AddDoc(docID string, value map[string]interface{}) error {
	return idx.AddDocPrefix(docID, value, "")
}

type entryValue struct {
	term     []byte
	termKey  []byte
	entryKey []byte
}

func newEntry(docID string, field string, value interface{}) entryValue {
	var term []byte
	if x, ok := value.(string); ok {
		term = []byte(x)
	}
	t := TermKey(field, term)
	ent := EntryKey(field, term, docID)
	return entryValue{term: term, termKey: t, entryKey: ent}
}

func fieldScan(docID string, doc map[string]interface{}, fieldPrefix string, fields []string, out chan entryValue) {
	for k, v := range doc {
		f := fmt.Sprintf("%s.%s", fieldPrefix, k)
		if containsPrefix(f, fields) {
			if x, ok := v.(map[string]interface{}); ok {
				fieldScan(docID, x, fmt.Sprintf("%s.%s", fieldPrefix, k), fields, out)
			} else if contains(f, fields) {
				out <- newEntry(docID, f, v)
			}
		}
	}
}

// AddDocPrefix add new document and prefix all the fields with `fieldPrefix` path
func (idx *KVIndex) AddDocPrefix(docID string, doc map[string]interface{}, fieldPrefix string) error {
	fields := idx.ListFields()
	values := make(chan entryValue, bufferSize)
	go func() {
		fieldScan(docID, doc, fieldPrefix, fields, values)
		close(values)
	}()
	docKey := DocKey(docID)
	idx.kv.Update(func(tx kvi.KVTransaction) error {
		sdoc := Doc{Entries: [][]byte{}}
		for v := range values {
			//log.Printf("Index %#v", v)
			tx.Set(v.entryKey, []byte{})
			tx.Set(v.termKey, []byte{})
			sdoc.Entries = append(sdoc.Entries, v.entryKey)
		}
		data, _ := proto.Marshal(&sdoc)
		tx.Set(docKey, data)
		return nil
	})

	return nil
}

// RemoveDoc removes a document from the index: TODO
func (idx *KVIndex) RemoveDoc(docID string) error {
	idx.kv.Update(func(tx kvi.KVTransaction) error {
		log.Printf("Deleteing: %s", docID)
		docKey := DocKey(docID)
		data, err := tx.Get(docKey)
		if err != nil {
			return nil
		}
		doc := Doc{}
		proto.Unmarshal(data, &doc)
		for _, entryKey := range doc.Entries {
			tx.Delete(entryKey)
		}
		tx.Delete(docKey)
		return nil
	})
	return nil
}

func term2Bytes(term interface{}) []byte {
	if x, ok := term.(string); ok {
		return []byte(x)
	}
	return nil
}

// GetTermMatch find all documents where field has the value
func (idx *KVIndex) GetTermMatch(field string, value interface{}) chan string {
	out := make(chan string, bufferSize)
	go func() {
		term := term2Bytes(value)
		entryPrefix := EntryValuePrefix(field, term)
		defer close(out)
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
				_, _, doc := EntryKeyParse(it.Key())
				out <- doc
			}
			return nil
		})
	}()
	return out
}

// FieldTerms list all unique terms held by a term
func (idx *KVIndex) FieldTerms(field string) chan interface{} {
	out := make(chan interface{}, bufferSize)
	go func() {
		termPrefix := TermPrefix(field)
		defer close(out)
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, entry := TermKeyParse(it.Key())
				out <- string(entry)
			}
			return nil
		})
	}()
	return out
}

// FieldTermCounts get all terms, and their counts for a particular field
func (idx *KVIndex) FieldTermCounts(field string) chan KVTermCount {
	terms := make(chan []byte, bufferSize)
	go func() {
		defer close(terms)
		termPrefix := TermPrefix(field)
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, term := TermKeyParse(it.Key())
				terms <- term
			}
			return nil
		})
	}()
	out := make(chan KVTermCount, bufferSize)
	go func() {
		defer close(out)
		for term := range terms {
			entryPrefix := EntryValuePrefix(field, term)
			var count int64
			idx.kv.View(func(it kvi.KVIterator) error {
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
