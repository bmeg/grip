package goja

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/jsengine/underscore"
	"github.com/bmeg/arachne/protoutil"
	"github.com/dop251/goja"
	"log"
)

type GojaRuntime struct {
	vm   *goja.Runtime
	call goja.Callable
}

var added = jsengine.AddEngine("goja", NewFunction)

func NewFunction(source string, imports []string) (jsengine.JSEngine, error) {

	vm := goja.New()
	for _, src := range imports {
		_, err := vm.RunString(src)
		if err != nil {
			return nil, err
		}
	}

	us, _ := underscore.Asset("underscore.js")
	if _, err := vm.RunString(string(us)); err != nil {
		return nil, err
	}

	_, err := vm.RunString("var userFunction = " + source)
	if err != nil {
		return nil, err
	}

	out := vm.Get("userFunction")
	f, callable := goja.AssertFunction(out)
	if !callable {
		return nil, fmt.Errorf("no Function")
	}
	return &GojaRuntime{vm, f}, nil
}

func (self *GojaRuntime) Call(input ...*aql.QueryResult) *aql.QueryResult {
	m := []goja.Value{}
	for _, i := range input {
		s := i.GetStruct()
		m_i := protoutil.AsMap(s)
		m = append(m, self.vm.ToValue(m_i))
	}
	value, err := self.call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	val := value.Export()
	log.Printf("function return: %#v", val)
	o := protoutil.AsStruct(val.(map[string]interface{}))
	return &aql.QueryResult{&aql.QueryResult_Struct{o}}
}

func (self *GojaRuntime) CallBool(input ...*aql.QueryResult) bool {
	m := []goja.Value{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			m_i := protoutil.AsMap(x.Edge.Properties)
			m = append(m, self.vm.ToValue(m_i))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			m_i := protoutil.AsMap(x.Vertex.Properties)
			m = append(m, self.vm.ToValue(m_i))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Struct); ok {
			m_i := protoutil.AsMap(x.Struct)
			m = append(m, self.vm.ToValue(m_i))
		}
	}
	value, err := self.call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	otto_val := value.ToBoolean()
	return otto_val
}

func (self *GojaRuntime) CallValueMapBool(input map[string]aql.QueryResult) bool {

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
	//log.Printf("Eval: %s", c)
	value, err := self.call(nil, self.vm.ToValue(c))
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	otto_val := value.ToBoolean()
	return otto_val
}

func (self *GojaRuntime) CallValueToVertex(input map[string]aql.QueryResult) []string {
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
	value, err := self.call(nil, self.vm.ToValue(c))
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	//if value.Class() == "Array" {
	goja_val := value.Export()
	if x, ok := goja_val.([]string); ok {
		out := make([]string, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	}
	if x, ok := goja_val.([]interface{}); ok {
		out := make([]string, len(x))
		for i := range x {
			out[i] = x[i].(string) //BUG: This is effing stupid, check types!!!!
		}
		return out
	}
	//}
	log.Printf("Weirdness: %s", value.ExportType())
	return []string{}
}
