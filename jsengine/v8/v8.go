
// +build v8

package v8

import (
	"encoding/json"
	"fmt"
	"github.com/augustoroman/v8"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/jsengine/underscore"
	"github.com/bmeg/arachne/protoutil"
	"log"
)

type V8Runtime struct {
	ctx  *v8.Context
	user *v8.Value
}

var added = jsengine.AddEngine("v8", NewFunction)

func NewFunction(source string, imports []string) (jsengine.JSEngine, error) {

	ctx := v8.NewIsolate().NewContext()
	for _, src := range imports {
		_, err := ctx.Eval(src, "")
		if err != nil {
			return nil, err
		}
	}

	us, _ := underscore.Asset("underscore.js")
	if _, err := ctx.Eval(string(us), "underscore.js"); err != nil {
		return nil, err
	}

	_, err := ctx.Eval("var userFunction = "+source, "user.js")
	if err != nil {
		return nil, err
	}

	f, err := ctx.Global().Get("userFunction")
	if err != nil {
		return nil, fmt.Errorf("Compile Error: %s", err)
	}
	return &V8Runtime{ctx, f}, nil
}

func (self *V8Runtime) Call(input ...*aql.QueryResult) *aql.QueryResult {
	m := []*v8.Value{}
	for _, i := range input {
		s := i.GetStruct()
		m_i := protoutil.AsMap(s)
		v, _ := self.ctx.Create(m_i)
		m = append(m, v)
	}
	value, err := self.user.Call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}

	//there has to be a better way
	val := map[string]interface{}{}
	jv, _ := value.MarshalJSON()
	json.Unmarshal(jv, &val)

	log.Printf("function return: %#v", val)
	o := protoutil.AsStruct(val)
	return &aql.QueryResult{&aql.QueryResult_Struct{o}}
}

func (self *V8Runtime) CallBool(input ...*aql.QueryResult) bool {
	m := []*v8.Value{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			m_i := protoutil.AsMap(x.Edge.Properties)
			v, _ := self.ctx.Create(m_i)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			m_i := protoutil.AsMap(x.Vertex.Properties)
			v, _ := self.ctx.Create(m_i)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Struct); ok {
			m_i := protoutil.AsMap(x.Struct)
			v, _ := self.ctx.Create(m_i)
			m = append(m, v)
		}
	}
	value, err := self.user.Call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	val := false
	jv, _ := value.MarshalJSON()
	json.Unmarshal(jv, &val)
	return val
}

func (self *V8Runtime) CallValueMapBool(input map[string]aql.QueryResult) bool {

	c := map[string]interface{}{}
	for k, v := range input {
		l := map[string]interface{}{}
		if x, ok := v.GetResult().(*aql.QueryResult_Edge); ok {
			l["gid"] = x.Edge.Gid
			l["from"] = x.Edge.From
			l["to"] = x.Edge.To
			l["label"] = x.Edge.Label
			l["data"] = protoutil.AsMap(x.Edge.Properties)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Vertex); ok {
			l["gid"] = x.Vertex.Gid
			l["label"] = x.Vertex.Label
			l["data"] = protoutil.AsMap(x.Vertex.Properties)
		}
		c[k] = l
	}
	v, _ := self.ctx.Create(c)
	value, err := self.user.Call(nil, v)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	val := false
	jv, _ := value.MarshalJSON()
	json.Unmarshal(jv, &val)
	return val
}

func (self *V8Runtime) CallValueToVertex(input map[string]aql.QueryResult) []string {
	c := map[string]interface{}{}
	for k, v := range input {
		l := map[string]interface{}{}
		if x, ok := v.GetResult().(*aql.QueryResult_Edge); ok {
			l["gid"] = x.Edge.Gid
			l["from"] = x.Edge.From
			l["to"] = x.Edge.To
			l["label"] = x.Edge.Label
			l["data"] = protoutil.AsMap(x.Edge.Properties)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Vertex); ok {
			l["gid"] = x.Vertex.Gid
			l["label"] = x.Vertex.Label
			l["data"] = protoutil.AsMap(x.Vertex.Properties)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Bundle); ok {
			l["gid"] = x.Bundle.Gid
			l["from"] = x.Bundle.From
			l["label"] = x.Bundle.Label
			b := map[string]interface{}{}
			for k, v := range x.Bundle.Bundle {
				b[k] = protoutil.AsMap(v)
			}
			l["bundle"] = b
		}
		c[k] = l
	}
	//log.Printf("Eval: %s", c)
	v, _ := self.ctx.Create(c)
	value, err := self.user.Call(nil, v)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	val := []string{}
	jv, _ := value.MarshalJSON()
	json.Unmarshal(jv, &val)
	return val
}
