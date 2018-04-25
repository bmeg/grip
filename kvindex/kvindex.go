package kvindex

import (
	//"context"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/bmeg/arachne/kvi"
	proto "github.com/golang/protobuf/proto"
)

// TermType defines in a term is a Number or a String
type TermType byte

const (
	//TermUnknown is an undefined term type
	TermUnknown TermType = 0x00
	//TermString means the term is a string
	TermString TermType = 0x01
	//TermNumber means the term is a number
	TermNumber TermType = 0x02
)

const bufferSize = 1000

//key: f | field
//val:
var idxFieldPrefix = []byte("f")

//key: t | field | TermType | term
//val:
//key: t | field | term
//val:
var idxTermPrefix = []byte("t")

//key: i | field | TermType | term | docid
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
func TermKey(field string, ttype TermType, term []byte) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), {byte(ttype)}, term}, []byte{0})
}

// TermPrefix get a prefix for all the terms for a single field
func TermPrefix(field string) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), {}}, []byte{0})
}

// TermTypePrefix get a prefix for all the terms for a single field
func TermTypePrefix(field string, ttype TermType) []byte {
	return bytes.Join([][]byte{idxTermPrefix, []byte(field), {byte(ttype)}, {}}, []byte{0})
}

// TermKeyParse parse a term key into a field and a value
func TermKeyParse(key []byte) (string, TermType, []byte) {
	tmp := bytes.SplitN(key, []byte{0}, 4)
	field := string(tmp[1])
	ttype := tmp[2][0]
	term := tmp[3]
	return field, TermType(ttype), term
}

// EntryKey create a key for an entry
func EntryKey(field string, ttype TermType, term []byte, docid string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {byte(ttype)}, term, []byte(docid)}, []byte{0})
}

// EntryPrefix get prefix for all entries for a single field
func EntryPrefix(field string) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {}}, []byte{0})
}

// EntryTypePrefix get prefix for all entries for a single field
func EntryTypePrefix(field string, ttype TermType) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {byte(ttype)}, {}}, []byte{0})
}

// EntryValuePrefix get prefix for all terms for a field
func EntryValuePrefix(field string, ttype TermType, term []byte) []byte {
	return bytes.Join([][]byte{idxEntryPrefix, []byte(field), {byte(ttype)}, term, {}}, []byte{0})
}

// EntryKeyParse take entry key and parse out field term and document id
func EntryKeyParse(key []byte) (string, TermType, []byte, string) {
	tmp := bytes.SplitN(key, []byte{0}, 4)
	field := string(tmp[1])
	ttype := TermType(tmp[2][0])
	suffix := tmp[3]
	if ttype == TermNumber {
		return field, ttype, suffix[0:8], string(suffix[8:])
	}
	stmp := bytes.Split(suffix, []byte{0})
	return field, ttype, stmp[0], string(stmp[1])
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
	kv     kvi.KVInterface
	fields map[string][]string
}

// KVTermCount Get all terms and their counts
type KVTermCount struct {
	String string
	Number float64
	Count  int64
}

// NewIndex create new key value index
func NewIndex(kv kvi.KVInterface) *KVIndex {
	out := &KVIndex{kv: kv}
	fields := out.ListFields()
	out.fields = make(map[string][]string, len(fields))
	for i := range fields {
		out.fields[fields[i]] = strings.Split(fields[i], ".")
	}
	return out
}

// AddField add new field to be indexed
func (idx *KVIndex) AddField(path string) error {
	fk := FieldKey(path)
	idx.fields[path] = strings.Split(path, ".")
	return idx.kv.Set(fk, []byte{})
}

// RemoveField removes an indexed field
func (idx *KVIndex) RemoveField(path string) error {
	fk := FieldKey(path)
	fkt := TermPrefix(path)
	ed := EntryPrefix(path)
	idx.kv.DeletePrefix(fkt)
	idx.kv.DeletePrefix(ed)
	delete(idx.fields, path)
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
	return idx.kv.Update(func(tx kvi.KVTransaction) error {
		return idx.AddDocTx(tx, docID, value)
	})
}

type entryValue struct {
	term     []byte
	termKey  []byte
	entryKey []byte
}

func newEntry(docID string, field string, value interface{}) entryValue {
	term, ttype := getTermBytes(value)
	t := TermKey(field, ttype, term)
	ent := EntryKey(field, ttype, term, docID)
	return entryValue{term: term, termKey: t, entryKey: ent}
}

func fieldScan(docID string, doc map[string]interface{}, fieldPrefix string, fields []string, out chan entryValue) {
	for k, v := range doc {
		f := fmt.Sprintf("%s.%s", fieldPrefix, k)
		//log.Printf("Checking %s in %s", f, fields)
		if containsPrefix(f, fields) {
			if x, ok := v.(map[string]interface{}); ok {
				fieldScan(docID, x, fmt.Sprintf("%s.%s", fieldPrefix, k), fields, out)
			} else if contains(f, fields) {
				out <- newEntry(docID, f, v)
			}
		}
	}
}

func mapDig(i map[string]interface{}, path []string) interface{} {
	//log.Printf("Digging %s", path)
	if x, ok := i[path[0]]; ok {
		if len(path) > 1 {
			if y, ok := x.(map[string]interface{}); ok {
				return mapDig(y, path[1:])
			}
		} else {
			return x
		}
	}
	return nil
}

func getTermBytes(term interface{}) ([]byte, TermType) {
	switch val := term.(type) {
	case string:
		return []byte(val), TermString
	case float64:
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, math.Float64bits(val))
		return out, TermNumber
	}
	return nil, TermUnknown
}

func getBytesTerm(val []byte, ttype TermType) interface{} {
	if ttype == TermString {
		return string(val)
	}
	if ttype == TermNumber {
		u := binary.BigEndian.Uint64(val)
		return math.Float64frombits(u)
	}
	return nil
}

// AddDocTx add new document using a transaction provided by user
func (idx *KVIndex) AddDocTx(tx kvi.KVTransaction, docID string, doc map[string]interface{}) error {
	sdoc := Doc{Entries: [][]byte{}}
	docKey := DocKey(docID)

	for field, p := range idx.fields {
		x := mapDig(doc, p)
		if x != nil {
			term, t := getTermBytes(x)
			if t != TermUnknown {
				entryKey := EntryKey(field, t, term, docID)
				termKey := TermKey(field, t, term)
				tx.Set(entryKey, []byte{})
				tx.Set(termKey, []byte{})
				sdoc.Entries = append(sdoc.Entries, entryKey)
			}
		}
	}
	data, _ := proto.Marshal(&sdoc)
	tx.Set(docKey, data)
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

// GetTermMatch find all documents where field has the value
func (idx *KVIndex) GetTermMatch(field string, value interface{}) chan string {
	out := make(chan string, bufferSize)
	go func() {
		term, ttype := getTermBytes(value)
		entryPrefix := EntryValuePrefix(field, ttype, term)
		defer close(out)
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
				_, _, _, doc := EntryKeyParse(it.Key())
				out <- doc
			}
			return nil
		})
	}()
	return out
}

// FieldTerms list all unique terms held by a field
func (idx *KVIndex) FieldTerms(field string) chan interface{} {
	out := make(chan interface{}, bufferSize)
	go func() {
		termPrefix := TermPrefix(field)
		defer close(out)
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, ttype, term := TermKeyParse(it.Key())
				out <- getBytesTerm(term, ttype)
			}
			return nil
		})
	}()
	return out
}

// FieldNumbers returns all entries values, in numeric order
func (idx *KVIndex) FieldNumbers(field string) chan float64 {
	out := make(chan float64, bufferSize)
	go func() {
		defer close(out)
		idx.kv.View(func(it kvi.KVIterator) error {
			//check negative
			ninf := EntryValuePrefix(field, TermNumber, floatNegInfBytes)
			inf := EntryValuePrefix(field, TermNumber, floatPosInfBytes)
			zero := EntryValuePrefix(field, TermNumber, floatZeroBytes)
			for it.SeekReverse(ninf); it.Valid() && bytes.Compare(inf, it.Key()) < 0; it.Next() {
				_, _, term := TermKeyParse(it.Key())
				val := getBytesTerm(term, TermNumber).(float64)
				out <- val
			}
			for it.Seek(zero); it.Valid() && bytes.Compare(inf, it.Key()) > 0; it.Next() {
				_, _, term := TermKeyParse(it.Key())
				val := getBytesTerm(term, TermNumber).(float64)
				out <- val
			}
			return nil
		})
	}()
	return out
}

type typedTerm struct {
	t    TermType
	term []byte
}

// FieldTermCounts get all terms, and their counts for a particular field
func (idx *KVIndex) fieldTermCounts(field string, ftype TermType) chan KVTermCount {
	terms := make(chan typedTerm, bufferSize)
	go func() {
		defer close(terms)
		termPrefix := TermTypePrefix(field, ftype)
		if ftype == TermUnknown {
			termPrefix = TermPrefix(field)
		}
		idx.kv.View(func(it kvi.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, ttype, term := TermKeyParse(it.Key())
				terms <- typedTerm{ttype, term}
			}
			return nil
		})
	}()
	out := make(chan KVTermCount, bufferSize)
	go func() {
		defer close(out)
		for term := range terms {
			entryPrefix := EntryValuePrefix(field, term.t, term.term)
			var count int64
			idx.kv.View(func(it kvi.KVIterator) error {
				for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
					count++
				}
				return nil
			})
			if term.t == TermNumber {
				out <- KVTermCount{Number: getBytesTerm(term.term, term.t).(float64), Count: count}
			} else {
				out <- KVTermCount{String: getBytesTerm(term.term, term.t).(string), Count: count}
			}
		}
	}()
	return out
}

// FieldTermCounts get all terms, and their counts for a particular field
func (idx *KVIndex) FieldTermCounts(field string) chan KVTermCount {
	return idx.fieldTermCounts(field, TermUnknown)
}

// FieldStringTermCounts get all terms, that are strings, and their counts for a particular field
func (idx *KVIndex) FieldStringTermCounts(field string) chan KVTermCount {
	return idx.fieldTermCounts(field, TermString)
}

var floatNegInfBytes, _ = getTermBytes(math.Inf(-1))
var floatPosInfBytes, _ = getTermBytes(math.Inf(1))
var floatZeroBytes, _ = getTermBytes(0.0)

// FieldTermNumberMin for a field, get the min number term value
func (idx *KVIndex) FieldTermNumberMin(field string) float64 {
	var min float64
	idx.kv.View(func(it kvi.KVIterator) error {
		prefix := EntryTypePrefix(field, TermNumber)
		//check negative
		ninf := EntryValuePrefix(field, TermNumber, floatNegInfBytes)
		it.SeekReverse(ninf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := getBytesTerm(term, TermNumber).(float64)
			if val < 0 {
				min = val
				return nil
			}
		}
		//check positive
		zero := EntryValuePrefix(field, TermNumber, floatZeroBytes)
		it.Seek(zero)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := getBytesTerm(term, TermNumber).(float64)
			if val >= 0 {
				min = val
				return nil
			}
		}
		return nil
	})
	return min
}

// FieldTermNumberMax finds the max number term for a field
func (idx *KVIndex) FieldTermNumberMax(field string) float64 {
	var min float64
	idx.kv.View(func(it kvi.KVIterator) error {
		prefix := EntryTypePrefix(field, TermNumber)
		//check positive
		inf := EntryValuePrefix(field, TermNumber, floatPosInfBytes)
		it.SeekReverse(inf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := getBytesTerm(term, TermNumber).(float64)
			log.Printf("MaxScan: %f", val)
			if val > 0 {
				min = val
				return nil
			}
		}
		//check negative
		it.Seek(inf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := getBytesTerm(term, TermNumber).(float64)
			if val < 0 {
				min = val
				return nil
			}
		}
		return nil
	})
	return min
}

//FieldTermNumberRange gets all number term counts between min and max
func (idx *KVIndex) FieldTermNumberRange(field string, min, max float64) chan KVTermCount {

	minBytes, _ := getTermBytes(min)
	maxBytes, _ := getTermBytes(max)
	out := make(chan KVTermCount, 100)
	defer close(out)
	if min > max {
		return out
	}

	if min < 0 {
		minPrefix := EntryValuePrefix(field, TermNumber, minBytes)
		maxPrefix := EntryValuePrefix(field, TermNumber, maxBytes)
		if max > 0 {
			maxPrefix = EntryValuePrefix(field, TermNumber, floatPosInfBytes)
		}
		idx.kv.View(func(it kvi.KVIterator) error {
			var count int64
			last := math.Inf(1)
			for it.SeekReverse(minPrefix); it.Valid() && bytes.Compare(maxPrefix, it.Key()) < 0; it.Next() {
				_, _, term, _ := EntryKeyParse(it.Key())
				val := getBytesTerm(term, TermNumber).(float64)
				if val != last {
					if count > 0 {
						out <- KVTermCount{Number: last, Count: count}
					}
					last = val
					count = 0
				}
				count++
			}
			if count > 0 {
				out <- KVTermCount{Number: last, Count: count}
			}
			return nil
		})
	}
	if max >= 0 {
		minPrefix := EntryValuePrefix(field, TermNumber, minBytes)
		if min < 0 {
			minPrefix = EntryValuePrefix(field, TermNumber, floatZeroBytes)
		}
		maxPrefix := EntryValuePrefix(field, TermNumber, maxBytes)
		idx.kv.View(func(it kvi.KVIterator) error {
			var count int64
			last := math.Inf(1)
			for it.Seek(minPrefix); it.Valid() && bytes.Compare(it.Key(), maxPrefix) < 0; it.Next() {
				_, _, term, _ := EntryKeyParse(it.Key())
				val := getBytesTerm(term, TermNumber).(float64)
				if val != last {
					if count > 0 {
						out <- KVTermCount{Number: last, Count: count}
					}
					last = val
					count = 0
				}
				count++
			}
			if count > 0 {
				out <- KVTermCount{Number: last, Count: count}
			}
			return nil
		})
	}

	return out
}
