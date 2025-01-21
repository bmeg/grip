package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/bmeg/grip/engine/logic"
)

type JSONCompare struct{}

// Compare implements logic.SortConf.
func (j *JSONCompare) Compare(a any, b any) int {
	return logic.CompareAny(a, b)
}

// FromBytes implements logic.SortConf.
func (j *JSONCompare) FromBytes(b []byte) any {
	if b[0] == '"' {
		var a string
		json.Unmarshal(b, &a)
		return a
	} else if strings.Compare(string(b), "true") == 0 {
		return true
	} else if strings.Compare(string(b), "false") == 0 {
		return false
	} else if strings.Contains(string(b), ".") {
		var a float64
		json.Unmarshal(b, &a)
		return a
	} else {
		var a int
		json.Unmarshal(b, &a)
		return a
	}
}

// ToBytes implements logic.SortConf.
func (j *JSONCompare) ToBytes(a any) []byte {
	o, _ := json.Marshal(a)
	return o
}

func TestKVSort(t *testing.T) {

	path := "kv_sort_test.store"

	keys := []any{
		"Bob",
		"Charles",
		"Alice",
		1.0,
		2,
		20.0,
		40,
		true,
		false,
	}

	jConf := JSONCompare{}

	memSort := logic.NewMemSorter[any](&jConf)
	for _, i := range keys {
		memSort.Add(i)
	}

	out1 := []any{}
	for i := range memSort.Sorted() {
		out1 = append(out1, i)
	}

	kvSort := logic.NewKVSorter(path, &jConf)
	for _, i := range keys {
		kvSort.Add(i)
	}
	out2 := []any{}
	for i := range kvSort.Sorted() {
		out2 = append(out2, i)
	}

	for i := range out1 {
		if logic.CompareAny(out1[i], out2[i]) != 0 {
			t.Error("mismatch")
		}
	}

	os.RemoveAll(path)
}
