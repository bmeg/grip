package aql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

// GetDataMap obtains data attached to vertex in the form of a map
func (vertex *Vertex) GetDataMap() map[string]interface{} {
	return protoutil.AsMap(vertex.Data)
}

// SetDataMap obtains data attached to vertex in the form of a map
func (vertex *Vertex) SetDataMap(i map[string]interface{}) {
	vertex.Data = protoutil.AsStruct(i)
}

// SetProperty sets named field in Vertex data
func (vertex *Vertex) SetProperty(key string, value interface{}) {
	if vertex.Data == nil {
		vertex.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	protoutil.StructSet(vertex.Data, key, value)
}

// GetProperty get named field from vertex data
func (vertex *Vertex) GetProperty(key string) interface{} {
	if vertex.Data == nil {
		return nil
	}
	m := protoutil.AsMap(vertex.Data)
	return m[key]
}

// HasProperty returns true is field is defined
func (vertex *Vertex) HasProperty(key string) bool {
	if vertex.Data == nil {
		return false
	}
	m := protoutil.AsMap(vertex.Data)
	_, ok := m[key]
	return ok
}

// Validate returns an error if the vertex is invalid
func (vertex *Vertex) Validate() error {
	if vertex.Gid == "" {
		return fmt.Errorf("'gid' cannot be blank")
	}
	if vertex.Label == "" {
		return fmt.Errorf("'label' cannot be blank")
	}
	for k := range vertex.GetDataMap() {
		for _, v := range []string{"gid", "label", "to", "from", "data"} {
			if k == v {
				return fmt.Errorf("data field '%s' uses a reserved name", k)
			}
		}
		if strings.Contains(k, ".") {
			return fmt.Errorf("data field '%s' invalid; fields cannot contain periods", k)
		}
	}
	return nil
}

// GetDataMap obtains data attached to vertex in the form of a map
func (edge *Edge) GetDataMap() map[string]interface{} {
	return protoutil.AsMap(edge.Data)
}

// SetDataMap obtains data attached to vertex in the form of a map
func (edge *Edge) SetDataMap(i map[string]interface{}) {
	edge.Data = protoutil.AsStruct(i)
}

// SetProperty sets named field in Vertex data
func (edge *Edge) SetProperty(key string, value interface{}) {
	if edge.Data == nil {
		edge.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	protoutil.StructSet(edge.Data, key, value)
}

// GetProperty get named field from edge data
func (edge *Edge) GetProperty(key string) interface{} {
	if edge.Data == nil {
		return nil
	}
	m := protoutil.AsMap(edge.Data)
	return m[key]
}

// HasProperty returns true is field is defined
func (edge *Edge) HasProperty(key string) bool {
	if edge.Data == nil {
		return false
	}
	m := protoutil.AsMap(edge.Data)
	_, ok := m[key]
	return ok
}

// Validate returns an error if the edge is invalid
func (edge *Edge) Validate() error {
	if edge.Gid == "" {
		return fmt.Errorf("'gid' cannot be blank")
	}
	if edge.Label == "" {
		return fmt.Errorf("'label' cannot be blank")
	}
	if edge.From == "" {
		return fmt.Errorf("'from' cannot be blank")
	}
	if edge.To == "" {
		return fmt.Errorf("'to' cannot be blank")
	}
	for k := range edge.GetDataMap() {
		for _, v := range []string{"gid", "label", "to", "from", "data"} {
			if k == v {
				return fmt.Errorf("data field '%s' uses a reserved name", k)
			}
		}
		if strings.Contains(k, ".") {
			return fmt.Errorf("data field '%s' invalid; fields cannot contain periods", k)
		}
	}
	return nil
}

// AsMap converts a NamedAggregationResult to a map[string]interface{}
func (namedAggRes *NamedAggregationResult) AsMap() map[string]interface{} {
	buckets := make([]map[string]interface{}, len(namedAggRes.Buckets))
	for i, b := range namedAggRes.Buckets {
		buckets[i] = b.AsMap()
	}

	return map[string]interface{}{
		"name":    namedAggRes.Name,
		"buckets": buckets,
	}
}

// AsMap converts an AggregationResult to a map[string]interface{}
func (aggRes *AggregationResult) AsMap() map[string]interface{} {
	return map[string]interface{}{
		"key":   aggRes.Key,
		"value": aggRes.Value,
	}
}

// SortedInsert inserts an AggregationResult into the Buckets field
// and returns the index of the insertion
func (namedAggRes *NamedAggregationResult) SortedInsert(el *AggregationResult) (int, error) {
	if !namedAggRes.IsValueSorted() {
		return 0, fmt.Errorf("buckets are not value sorted")
	}

	if len(namedAggRes.Buckets) == 0 {
		namedAggRes.Buckets = []*AggregationResult{el}
		return 0, nil
	}

	index := sort.Search(len(namedAggRes.Buckets), func(i int) bool {
		if namedAggRes.Buckets[i] == nil {
			return true
		}
		return el.Value > namedAggRes.Buckets[i].Value
	})

	namedAggRes.Buckets = append(namedAggRes.Buckets, &AggregationResult{})
	copy(namedAggRes.Buckets[index+1:], namedAggRes.Buckets[index:])
	namedAggRes.Buckets[index] = el

	return index, nil
}

// SortOnValue sorts Buckets by Value in descending order
func (namedAggRes *NamedAggregationResult) SortOnValue() {
	sort.Slice(namedAggRes.Buckets, func(i, j int) bool {
		if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] != nil {
			return true
		}
		if namedAggRes.Buckets[i] != nil && namedAggRes.Buckets[j] == nil {
			return false
		}
		if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] == nil {
			return false
		}
		return namedAggRes.Buckets[i].Value > namedAggRes.Buckets[j].Value
	})
}

// IsValueSorted returns true if the Buckets are sorted by Value
func (namedAggRes *NamedAggregationResult) IsValueSorted() bool {
	for i := range namedAggRes.Buckets {
		j := i + 1
		if i < len(namedAggRes.Buckets)-2 {
			if namedAggRes.Buckets[i] != nil && namedAggRes.Buckets[j] == nil {
				return true
			}
			if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] != nil {
				return false
			}
			if namedAggRes.Buckets[i] == nil && namedAggRes.Buckets[j] == nil {
				return true
			}
			if namedAggRes.Buckets[i].Value < namedAggRes.Buckets[j].Value {
				return false
			}
		}
	}
	return true
}

// ValidateGraphName returns an error if the graph name is invalid
func ValidateGraphName(graph string) error {
	if strings.ContainsAny(graph, `/\. "'$*<>:|?`) {
		return fmt.Errorf(`invalid name; cannot contain /\. "'$*<>:|?`)
	}
	if strings.HasPrefix(graph, "_") || strings.HasPrefix(graph, "+") || strings.HasPrefix(graph, "-") {
		return fmt.Errorf(`invalid name; cannot start with _-+`)
	}
	return nil
}
