package gripql

import (
	"errors"
	"fmt"

	//"sort"
	"strings"

	"github.com/bmeg/grip/protoutil"
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
		return errors.New("'gid' cannot be blank")
	}
	if vertex.Label == "" {
		return errors.New("'label' cannot be blank")
	}
	for k := range vertex.GetDataMap() {
		err := ValidateFieldName(k)
		if err != nil {
			return err
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
		return errors.New("'gid' cannot be blank")
	}
	if edge.Label == "" {
		return errors.New("'label' cannot be blank")
	}
	if edge.From == "" {
		return errors.New("'from' cannot be blank")
	}
	if edge.To == "" {
		return errors.New("'to' cannot be blank")
	}
	for k := range edge.GetDataMap() {
		err := ValidateFieldName(k)
		if err != nil {
			return err
		}
	}
	return nil
}

// ValidateGraphName returns an error if the graph name is invalid
func ValidateGraphName(graph string) error {
	err := validate(graph)
	if err != nil {
		return fmt.Errorf(`invalid graph name %s; %v`, graph, err)
	}
	return nil
}

// ReservedFields are the fields that cannot be used as keys within the data of a vertex or edge
var ReservedFields = []string{"_gid", "_label", "_to", "_from", "_data"}

// ValidateFieldName returns an error if the data field name is invalid
func ValidateFieldName(k string) error {
	for _, v := range ReservedFields {
		if k == v {
			return fmt.Errorf("data field '%s' uses a reserved name", k)
		}
	}
	err := validate(k)
	if err != nil {
		return fmt.Errorf(`invalid data field '%s'; %v`, k, err)
	}
	return nil
}

func validate(k string) error {
	if strings.ContainsAny(k, `!@#$%^&*()+={}[] :;"',.<>?/\|~`) {
		return errors.New(`cannot contain: !@#$%^&*()+={}[] :;"',.<>?/\|~`)
	}
	if strings.HasPrefix(k, "_") || strings.HasPrefix(k, "-") {
		return errors.New(`cannot start with _-`)
	}
	return nil
}
