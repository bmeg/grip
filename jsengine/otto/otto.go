package ottoengine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/protoutil"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"log"
)

type CompiledFunction struct {
	Function otto.Value
}

var added = jsengine.AddEngine("otto", NewFunction)

func NewFunction(source string, imports []string) (jsengine.JSEngine, error) {

	vm := otto.New()
	for _, src := range imports {
		_, err := vm.Run(src)
		if err != nil {
			return nil, err
		}
	}

	_, err := vm.Run("var userFunction = " + source)
	if err != nil {
		return nil, err
	}

	out, err := vm.Get("userFunction")
	if err != nil {
		return nil, err
	}

	if out.IsFunction() {
		return &CompiledFunction{Function: out}, nil
	}
	return nil, fmt.Errorf("no Function")
}

func (self *CompiledFunction) Call(input ...*aql.QueryResult) *aql.QueryResult {

	m := []interface{}{}
	for _, i := range input {
		s := i.GetStruct()
		m_i := protoutil.AsMap(s)
		m = append(m, m_i)
	}

	//log.Printf("Inputs: %#v", m)
	//log.Printf("Function: %#v", self.Function)
	/*
	 // code to deal with panics inside of the JS engine
	  defer func() {
	       if r := recover(); r != nil {
	           fmt.Println("Recovered in f", r)
	       }
	   }()
	*/
	value, err := self.Function.Call(otto.Value{}, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}

	otto_val, _ := value.Export()

	//struct_val := otto2map(otto_val)
	log.Printf("function return: %#v", otto_val)
	o := protoutil.AsStruct(otto_val.(map[string]interface{}))
	return &aql.QueryResult{&aql.QueryResult_Struct{o}}
}

func (self *CompiledFunction) CallBool(input ...*aql.QueryResult) bool {

	m := []interface{}{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			m_i := protoutil.AsMap(x.Edge.Data)
			m = append(m, m_i)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			m_i := protoutil.AsMap(x.Vertex.Data)
			m = append(m, m_i)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Struct); ok {
			m_i := protoutil.AsMap(x.Struct)
			m = append(m, m_i)
		}
	}

	//log.Printf("Inputs: %#v", m)
	//log.Printf("Function: %#v", self.Function)
	/*
	 // code to deal with panics inside of the JS engine
	  defer func() {
	       if r := recover(); r != nil {
	           fmt.Println("Recovered in f", r)
	       }
	   }()
	*/
	value, err := self.Function.Call(otto.Value{}, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	otto_val, _ := value.ToBoolean()
	return otto_val
}

func (self *CompiledFunction) CallValueMapBool(input map[string]aql.QueryResult) bool {

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
	//log.Printf("Eval: %s", c)
	value, err := self.Function.Call(otto.Value{}, c)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	otto_val, _ := value.ToBoolean()
	return otto_val
}

func (self *CompiledFunction) CallValueToVertex(input map[string]aql.QueryResult) []string {
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
	value, err := self.Function.Call(otto.Value{}, c)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	if value.Class() == "Array" {
		otto_val, _ := value.Export()
		if x, ok := otto_val.([]string); ok {
			out := make([]string, len(x))
			for i := range x {
				out[i] = x[i]
			}
			return out
		}
	}
	log.Printf("Weirdness: %s", value.Class())
	return []string{}
}

func otto2map(obj *otto.Object) map[string]interface{} {
	out := map[string]interface{}{}
	for _, i := range obj.Keys() {
		val, _ := obj.Get(i)
		if val.IsBoolean() {
			out[i], _ = val.ToBoolean()
		}
		if val.IsBoolean() {
			out[i], _ = val.ToBoolean()
		}
		if val.IsString() {
			out[i], _ = val.ToString()
		}
		if val.IsNumber() {
			out[i], _ = val.Export()
		}
		if val.IsObject() {
			out[i] = otto2map(val.Object())
		}
	}
	return out
}
