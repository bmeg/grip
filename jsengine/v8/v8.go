// +build v8

package v8

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/augustoroman/v8"
	"github.com/bmeg/grip/aql"
	"github.com/bmeg/grip/jsengine"
	"github.com/bmeg/grip/jsengine/underscore"
	"github.com/bmeg/grip/protoutil"
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
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			mI := protoutil.AsMap(x.Edge.Data)
			v, _ := self.ctx.Create(mI)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			mI := protoutil.AsMap(x.Vertex.Data)
			v, _ := self.ctx.Create(mI)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Data); ok {
			mI := protoutil.UnWrapValue(x.Data)
			v, _ := self.ctx.Create(mI)
			m = append(m, v)
		}
	}
	value, err := self.user.Call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}

	//there has to be a better way
	val := map[string]interface{}{}
	jv, _ := value.MarshalJSON()
	json.Unmarshal(jv, &val)

	//log.Printf("function return: %#v", val)
	o := protoutil.WrapValue(val)
	return &aql.QueryResult{&aql.QueryResult_Data{o}}
}

func (self *V8Runtime) CallBool(input ...*aql.QueryResult) bool {
	m := []*v8.Value{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			m_i := protoutil.AsMap(x.Edge.Data)
			v, _ := self.ctx.Create(m_i)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			m_i := protoutil.AsMap(x.Vertex.Data)
			v, _ := self.ctx.Create(m_i)
			m = append(m, v)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Data); ok {
			m_i := protoutil.UnWrapValue(x.Data)
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
			l["data"] = protoutil.AsMap(x.Edge.Data)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Vertex); ok {
			l["gid"] = x.Vertex.Gid
			l["label"] = x.Vertex.Label
			l["data"] = protoutil.AsMap(x.Vertex.Data)
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
			l["data"] = protoutil.AsMap(x.Edge.Data)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Vertex); ok {
			l["gid"] = x.Vertex.Gid
			l["label"] = x.Vertex.Label
			l["data"] = protoutil.AsMap(x.Vertex.Data)
		}
		c[k] = l
	}

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
