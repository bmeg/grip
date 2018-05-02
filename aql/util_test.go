package aql

import (
	"testing"
)

func TestNamedAggregationResultInsert(t *testing.T) {
	size := 5
	aggRes := NamedAggregationResult{
		Name:    "test",
		Buckets: make([]*AggregationResult, size),
	}

	for i := 0; i < 5; i++ {
		aggRes.SortedInsert(&AggregationResult{Value: float64((i + 1) * 2)})
	}

	for i := range aggRes.Buckets {
		if i < len(aggRes.Buckets)-2 {
			if aggRes.Buckets[i].Value < aggRes.Buckets[i+1].Value {
				t.Errorf("unexpected bucket order %+v", aggRes.Buckets)
			}
		}
	}

	index := aggRes.SortedInsert(&AggregationResult{Value: float64(5)})
	if len(aggRes.Buckets) != size {
		t.Errorf("unexpected list size %d != %d", size, len(aggRes.Buckets))
	}
	if index != 3 {
		t.Errorf("incorrect index returned %d != %d", 3, index)
	}
	if aggRes.Buckets[index].Value != 5 {
		t.Errorf("unexpected value in list:  %+v ", aggRes.Buckets)
	}
}

func TestNamedAggregationResultSort(t *testing.T) {
	size := 5
	aggRes := NamedAggregationResult{
		Name:    "test",
		Buckets: make([]*AggregationResult, size),
	}

	for i := 0; i < 5; i++ {
		aggRes.Buckets[i] = &AggregationResult{Value: float64((i + 1) * 2)}
	}

	t.Logf("initial list: %+v", aggRes.Buckets)
	aggRes.SortOnValue()
	t.Logf("sorted list: %+v", aggRes.Buckets)
	for i := range aggRes.Buckets {
		if i < len(aggRes.Buckets)-2 {
			if aggRes.Buckets[i].Value < aggRes.Buckets[i+1].Value {
				t.Errorf("unexpected bucket order %+v", aggRes.Buckets)
			}
		}
	}
}
