package kvindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/log"

	"google.golang.org/protobuf/proto"
)

const bufferSize = 1000

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
	KV     kvi.KVInterface
	Fields map[string][]string
}

// KVTermCount Get all terms and their counts
type KVTermCount struct {
	String string
	Number float64
	Count  uint64
}

// NewIndex create new key value index
func NewIndex(kv kvi.KVInterface) *KVIndex {
	return &KVIndex{KV: kv, Fields: make(map[string][]string)}
}

// AddField add new field to be indexed
func (idx *KVIndex) AddField(path string) error {
	fk := FieldKey(path)
	idx.Fields[path] = strings.Split(path, ".")
	return idx.KV.Set(fk, []byte{})
}

// RemoveField removes an indexed field
func (idx *KVIndex) RemoveField(path string) error {
	fk := FieldKey(path)
	fkt := TermPrefix(path)
	ed := EntryPrefix(path)
	idx.KV.DeletePrefix(fkt)
	idx.KV.DeletePrefix(ed)
	delete(idx.Fields, path)
	return idx.KV.Delete(fk)
}

// ListFields lists all indexed fields
func (idx *KVIndex) ListFields() []string {
	out := make([]string, 0, 10)
	fPrefix := FieldPrefix()
	idx.KV.View(func(it kvi.KVIterator) error {
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
	err := idx.KV.Update(func(tx kvi.KVTransaction) error {
		return idx.AddDocTx(tx, docID, value)
	})
	if err != nil {
		return fmt.Errorf("AddDoc call failed: %v", err)
	}
	return nil
}

// AddDocTx add new document using a transaction provided by user
func (idx *KVIndex) AddDocTx(tx kvi.KVBulkWrite, docID string, doc map[string]interface{}) error {
	sdoc := Doc{Entries: [][]byte{}}
	docKey := DocKey(docID)

	for field, p := range idx.Fields {
		x := mapDig(doc, p)
		if x != nil {
			term, t := GetTermBytes(x)
			switch t {
			case TermString, TermNumber:
				entryKey := EntryKey(field, t, term, docID)
				err := tx.Set(entryKey, []byte{})
				if err != nil {
					return fmt.Errorf("failed to set entry key %s: %v", entryKey, err)
				}
				sdoc.Entries = append(sdoc.Entries, entryKey)

				termKey := TermKey(field, t, term)
				//set the term count to 0 to invalidate it. Later on, if other code trying
				//to get the term count will have to recount
				//previously, it was a set(get+1), but for bulk loading, its better
				//to just write and never look things up
				var count uint64
				buf := make([]byte, binary.MaxVarintLen64)
				binary.PutUvarint(buf, count)
				err = tx.Set(termKey, buf)
				if err != nil {
					return fmt.Errorf("failed to set term key %s: %v", termKey, err)
				}

			default:
				return fmt.Errorf("unsupported term type")
			}
		}
	}

	data, err := proto.Marshal(&sdoc)
	if err != nil {
		return fmt.Errorf("failed to marshal document %s: %v", docKey, err)
	}
	err = tx.Set(docKey, data)
	if err != nil {
		return fmt.Errorf("failed to set document key %s: %v", docKey, err)
	}
	return nil
}

func (idx *KVIndex) termGetCount(tx kvi.KVTransaction, field string, ttype TermType, term []byte) (uint64, error) {
	termKey := TermKey(field, ttype, term)
	var count uint64
	i, err := tx.Get(termKey)
	if err == nil {
		count, _ = binary.Uvarint(i)
	} else {
		return 0, err
	}
	//if the term is zero, it needs to be recounted
	if count == 0 {
		entryPrefix := EntryValuePrefix(field, ttype, term)
		tx.View(func(it kvi.KVIterator) error {
			for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
				count++
			}
			return nil
		})
		//FIXME: technically, setting the value here is redundant, they only piece of code that currently
		//calls this function alters and sets the the value anyway, so you end up with an extra 'set'
		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutUvarint(buf, count)
		err = tx.Set(termKey, buf)
		if err != nil {
			log.Errorf("Change count error: %s", err)
			return 0, err
		}
	}
	return count, nil
}

// RemoveDoc removes a document from the index: TODO
func (idx *KVIndex) RemoveDoc(docID string) error {
	err := idx.KV.Update(func(tx kvi.KVTransaction) error {
		log.WithFields(log.Fields{"document_id": docID}).Debug("KVIndex: deleting document")
		docKey := DocKey(docID)
		data, err := tx.Get(docKey)
		if err != nil {
			return nil
		}
		doc := Doc{}
		err = proto.Unmarshal(data, &doc)
		if err != nil {
			return fmt.Errorf("failed to unmarshal document: %v", err)
		}
		for _, entryKey := range doc.Entries {
			err = tx.Delete(entryKey)
			if err != nil {
				return fmt.Errorf("failed to delete entry %s: %v", entryKey, err)
			}

			field, ttype, term, _ := EntryKeyParse(entryKey)
			termKey := TermKey(field, ttype, term)
			if count, err := idx.termGetCount(tx, field, ttype, term); err == nil {
				if count > 0 {
					count = count - 1
				}
				//if count == 0, then the term should be removed from the index
				if count == 0 {
					err = tx.Delete(termKey)
					if err != nil {
						return fmt.Errorf("failed to delete term key %s: %v", termKey, err)
					}
				} else {
					buf := make([]byte, binary.MaxVarintLen64)
					binary.PutUvarint(buf, count)
					err = tx.Set(termKey, buf)
					if err != nil {
						return fmt.Errorf("failed to set term key %s: %v", termKey, err)
					}
				}
			} else {
				return fmt.Errorf("Termcount Error: %s", err)
			}
		}

		err = tx.Delete(docKey)
		if err != nil {
			return fmt.Errorf("failed to delete document %s: %v", docKey, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("RemoveDoc call failed: %v", err)
	}
	return nil
}

// GetTermMatch find all documents where field has the value
func (idx *KVIndex) GetTermMatch(ctx context.Context, field string, value interface{}, maxCount int) chan string {
	out := make(chan string, bufferSize)
	go func() {
		term, ttype := GetTermBytes(value)
		entryPrefix := EntryValuePrefix(field, ttype, term)
		defer close(out)
		count := 0
		idx.KV.View(func(it kvi.KVIterator) error {
			for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				_, _, _, doc := EntryKeyParse(it.Key())
				out <- doc
				count++
				if maxCount > 0 && count >= maxCount {
					return nil
				}
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
		idx.KV.View(func(it kvi.KVIterator) error {
			for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
				_, ttype, term := TermKeyParse(it.Key())
				out <- GetBytesTerm(term, ttype)
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
		idx.KV.View(func(it kvi.KVIterator) error {
			//check negative
			prefix := EntryPrefix(field)
			ninf := EntryValuePrefix(field, TermNumber, floatNegInfBytes)
			inf := EntryValuePrefix(field, TermNumber, floatPosInfBytes)
			zero := EntryValuePrefix(field, TermNumber, floatZeroBytes)
			for it.SeekReverse(ninf); it.Valid() && bytes.HasPrefix(it.Key(), prefix) && bytes.Compare(inf, it.Key()) < 0; it.Next() {
				_, _, term := TermKeyParse(it.Key())
				val := GetBytesTerm(term, TermNumber).(float64)
				out <- val
			}
			for it.Seek(zero); it.Valid() && bytes.HasPrefix(it.Key(), prefix) && bytes.Compare(inf, it.Key()) > 0; it.Next() {
				_, _, term := TermKeyParse(it.Key())
				val := GetBytesTerm(term, TermNumber).(float64)
				out <- val
			}
			return nil
		})
	}()
	return out
}

// FieldTermCounts get all terms, and their counts for a particular field
func (idx *KVIndex) fieldTermCounts(field string, ftype TermType) chan KVTermCount {
	out := make(chan KVTermCount, bufferSize)
	go func() {
		defer close(out)
		termPrefix := TermTypePrefix(field, ftype)
		if ftype == TermUnknown {
			termPrefix = TermPrefix(field)
		}
		idx.KV.Update(func(tx kvi.KVTransaction) error {
			tx.View(func(it kvi.KVIterator) error {
				for it.Seek(termPrefix); it.Valid() && bytes.HasPrefix(it.Key(), termPrefix); it.Next() {
					curKey := it.Key()
					_, ttype, term := TermKeyParse(curKey)
					countBytes, _ := it.Value()
					count, _ := binary.Uvarint(countBytes)
					//if we encounter a count == 0, it means the value was invalidated and needs to be recalculated
					if count == 0 {
						entryPrefix := EntryValuePrefix(field, ttype, term)
						for it.Seek(entryPrefix); it.Valid() && bytes.HasPrefix(it.Key(), entryPrefix); it.Next() {
							count++
						}
						buf := make([]byte, binary.MaxVarintLen64)
						binary.PutUvarint(buf, count)
						tx.Set(curKey, buf)
						it.Seek(curKey)
					}
					switch ttype {
					case TermNumber:
						out <- KVTermCount{Number: GetBytesTerm(term, ttype).(float64), Count: count}
					case TermString:
						out <- KVTermCount{String: GetBytesTerm(term, ttype).(string), Count: count}
					default:
						continue
					}
				}
				return nil
			})
			return nil
		})
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

var floatNegInfBytes, _ = GetTermBytes(math.Inf(-1))
var floatPosInfBytes, _ = GetTermBytes(math.Inf(1))
var floatZeroBytes, _ = GetTermBytes(0.0)

// FieldTermNumberMin for a field, get the min number term value
func (idx *KVIndex) FieldTermNumberMin(field string) float64 {
	var min float64
	idx.KV.View(func(it kvi.KVIterator) error {
		prefix := EntryTypePrefix(field, TermNumber)
		//check negative
		ninf := EntryValuePrefix(field, TermNumber, floatNegInfBytes)
		it.SeekReverse(ninf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := GetBytesTerm(term, TermNumber).(float64)
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
			val := GetBytesTerm(term, TermNumber).(float64)
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
	idx.KV.View(func(it kvi.KVIterator) error {
		prefix := EntryTypePrefix(field, TermNumber)
		//check positive
		inf := EntryValuePrefix(field, TermNumber, floatPosInfBytes)
		it.SeekReverse(inf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := GetBytesTerm(term, TermNumber).(float64)
			log.WithFields(log.Fields{"field": field}).Debugf("KVIndex: FieldTermNumberMax: MaxScan: %f", val)
			if val > 0 {
				min = val
				return nil
			}
		}
		//check negative
		it.Seek(inf)
		if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
			_, _, term := TermKeyParse(it.Key())
			val := GetBytesTerm(term, TermNumber).(float64)
			if val < 0 {
				min = val
				return nil
			}
		}
		return nil
	})
	return min
}

// FieldTermNumberRange gets all number term counts between min and max
func (idx *KVIndex) FieldTermNumberRange(field string, min, max float64) chan KVTermCount {

	minBytes, _ := GetTermBytes(min)
	maxBytes, _ := GetTermBytes(max)
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
		idx.KV.View(func(it kvi.KVIterator) error {
			var count uint64
			last := math.Inf(1)
			for it.SeekReverse(minPrefix); it.Valid() && bytes.Compare(maxPrefix, it.Key()) < 0; it.Next() {
				_, _, term, _ := EntryKeyParse(it.Key())
				val := GetBytesTerm(term, TermNumber).(float64)
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
		idx.KV.View(func(it kvi.KVIterator) error {
			var count uint64
			last := math.Inf(1)
			for it.Seek(minPrefix); it.Valid() && bytes.Compare(it.Key(), maxPrefix) < 0; it.Next() {
				_, _, term, _ := EntryKeyParse(it.Key())
				val := GetBytesTerm(term, TermNumber).(float64)
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
