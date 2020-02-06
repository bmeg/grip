package kvindex

import (
  "bytes"
)

//Fields
//key: f | field
//val:
var idxFieldPrefix = []byte("f")

//Terms
//key: t | field | TermType | term
//val: count
var idxTermPrefix = []byte("t")

//Entries
//key: i | field | TermType | term | docid
//val:
var idxEntryPrefix = []byte("i")

//Docs
//key: d | docid
//val: Doc entry list
var idxDocPrefix = []byte("D")

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

// TermTypePrefix get a prefix for all the terms of a particular type for a single field
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
