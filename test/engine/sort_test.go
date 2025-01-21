package main

import (
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/bmeg/grip/engine/logic"
	"github.com/cockroachdb/pebble"
)

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
	slices.SortFunc(keys, logic.CompareAny)

	for _, i := range keys {
		fmt.Printf("%#v\n", i)
	}

	c := *pebble.DefaultComparer

	c.Compare = logic.CompareEncoded

	db, err := pebble.Open(path, &pebble.Options{Comparer: &c})
	if err != nil {
		t.Errorf("error: %s\n", err)
	}

	fmt.Printf("===DB sort test===\n")

	for _, i := range keys {
		db.Set(logic.EncodeAny(i), []byte{}, nil)
	}

	iter, err := db.NewIter(nil)
	if err != nil {
		fmt.Printf("error: %s\n", err)
	}
	out := []any{}
	for iter.First(); iter.Valid(); iter.Next() {
		j := logic.DecodeAny(iter.Key())
		out = append(out, j)
		fmt.Printf("%#v\n", j)
	}

	for i := range keys {
		if logic.CompareAny(keys[i], out[i]) != 0 {
			t.Error("mismatch")
		}
	}
	iter.Close()
	db.Close()
	os.RemoveAll(path)
}
