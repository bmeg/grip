package jsengine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	"github.com/robertkrimen/otto"
	"log"
)

type CompiledFunction struct {
	Function otto.Value
}

func NewFunction(source string, imports []string) (CompiledFunction, error) {

	vm := otto.New()
	for _, src := range imports {
		_, err := vm.Run(src)
		if err != nil {
			return CompiledFunction{}, err
		}
	}

	_, err := vm.Run("var userFunction = " + source)
	if err != nil {
		return CompiledFunction{}, err
	}

	out, err := vm.Get("userFunction")
	if err != nil {
		return CompiledFunction{}, err
	}

	if out.IsFunction() {
		return CompiledFunction{Function: out}, nil
	}
	return CompiledFunction{}, fmt.Errorf("no Function")
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
			m_i := protoutil.AsMap(x.Edge.Properties)
			m = append(m, m_i)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			m_i := protoutil.AsMap(x.Vertex.Properties)
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
			l["data"] = protoutil.AsMap(x.Edge.Properties)
		} else if x, ok := v.GetResult().(*aql.QueryResult_Vertex); ok {
			l["gid"] = x.Vertex.Gid
			l["label"] = x.Vertex.Label
			l["data"] = protoutil.AsMap(x.Vertex.Properties)
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
