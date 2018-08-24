package gripql

import (
	"testing"
)

func TestNamedAggregationResultInsert(t *testing.T) {
	size := 5
	aggRes := AggregationResult{
		Buckets: []*AggregationResultBucket{},
	}

	for i := 0; i < size; i++ {
		aggRes.Buckets = append(aggRes.Buckets, &AggregationResultBucket{Value: float64((i + 1) * 2)})
	}

	t.Logf("initial list: %v", aggRes.Buckets)

	index, err := aggRes.SortedInsert(&AggregationResultBucket{Value: float64(5)})
	if err == nil {
		t.Error("expected error for SortedInsert")
	}

	aggRes.SortOnValue()
	t.Logf("sorted initial list: %v", aggRes.Buckets)

	index, err = aggRes.SortedInsert(&AggregationResultBucket{Value: float64(5)})
	if err != nil {
		t.Error("unexpected error for SortedInsert", err)
	}
	t.Logf("list after insert: %v", aggRes.Buckets)

	if len(aggRes.Buckets) != size+1 {
		t.Errorf("unexpected list size %d != %d", size+1, len(aggRes.Buckets))
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
	aggRes := AggregationResult{
		Buckets: make([]*AggregationResultBucket, size),
	}

	for i := 0; i < 5; i++ {
		aggRes.Buckets[i] = &AggregationResultBucket{Value: float64((i + 1) * 2)}
	}

	t.Logf("initial list: %+v", aggRes.Buckets)
	aggRes.SortOnValue()
	t.Logf("sorted list: %+v", aggRes.Buckets)

	if !aggRes.IsValueSorted() {
		t.Errorf("unexpected bucket order %+v", aggRes.Buckets)
	}

	for i := range aggRes.Buckets {
		if i < len(aggRes.Buckets)-2 {
			if aggRes.Buckets[i].Value < aggRes.Buckets[i+1].Value {
				t.Errorf("unexpected bucket order %+v", aggRes.Buckets)
			}
		}
	}
}
