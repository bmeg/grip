package indexer

import (
	"bytes"
	"encoding/binary"

	"github.com/bmeg/benchtop"
	"github.com/bytedance/sonic"
)

type Entry struct {
	Key   []byte
	Value []byte
}

func PresenceKeys(tableID uint16, field string) (forwardKey []byte, reversePrefix []byte) {
	forwardKey = benchtop.FieldKey(field, tableID, nil, nil)
	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	reversePrefix = bytes.Join([][]byte{benchtop.RFieldPrefix, idBytes, []byte(field)}, benchtop.FieldSep)
	return forwardKey, reversePrefix
}

func BuildFieldIndexEntries(tableID uint16, field, rowID string, fieldValue any, rowLoc *benchtop.RowLoc) []Entry {
	entries := []Entry{
		{
			Key:   benchtop.FieldKey(field, tableID, fieldValue, []byte(rowID)),
			Value: benchtop.EncodeRowLoc(rowLoc),
		},
	}
	bVal, err := sonic.ConfigFastest.Marshal(fieldValue)
	if err != nil {
		return entries
	}
	entries = append(entries, Entry{
		Key:   benchtop.RFieldKey(tableID, field, rowID),
		Value: bVal,
	})
	return entries
}

func DecodeReverseFieldValue(v []byte) (any, error) {
	var out any
	if len(v) == 0 {
		return nil, nil
	}
	if err := sonic.ConfigFastest.Unmarshal(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}
