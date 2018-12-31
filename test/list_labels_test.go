package test

import (
	"reflect"
	"sort"
	"testing"
)

func TestListLabels(t *testing.T) {
	result, err := db.ListVertexLabels()
	if err != nil {
		t.Error(err)
	}
	expected := []string{"products", "purchases", "users"}
	sort.Strings(result)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("unexpected vertex labels: %s != %s", result, expected)
	}
	result, err = db.ListEdgeLabels()
	if err != nil {
		t.Error(err)
	}
	expected = []string{"purchasedProducts", "userPurchases"}
	sort.Strings(result)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("unexpected edge labels: %s != %s", result, expected)
	}
}
