package kvindex

import (
	"encoding/binary"
	"fmt"
	"math"
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

type entryValue struct {
	term     []byte
	termKey  []byte
	entryKey []byte
}

func newEntry(docID string, field string, value interface{}) entryValue {
	term, ttype := GetTermBytes(value)
	t := TermKey(field, ttype, term)
	ent := EntryKey(field, ttype, term, docID)
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

func mapDig(i map[string]interface{}, path []string) interface{} {
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

// GetTermBytes converts a term into its bytes representation and returns its type
func GetTermBytes(term interface{}) ([]byte, TermType) {
	switch val := term.(type) {
	case string:
		return []byte(val), TermString

	case float64:
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, math.Float64bits(val))
		return out, TermNumber

	default:
		return nil, TermUnknown
	}
}

// GetBytesTerm converts the bytes representation of a term back to its original value
func GetBytesTerm(val []byte, ttype TermType) interface{} {
	switch ttype {
	case TermString:
		return string(val)

	case TermNumber:
		u := binary.BigEndian.Uint64(val)
		return math.Float64frombits(u)

	default:
		return nil
	}
}
