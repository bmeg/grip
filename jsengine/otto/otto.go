package ottoengine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/protoutil"
	"github.com/robertkrimen/otto"
	//"github.com/robertkrimen/otto/underscore"
	"log"
)

type compiledFunction struct {
	Function otto.Value
}

var added = jsengine.AddEngine("otto", NewFunction)

// NewFunction returns a new javascript evaluator based on the `source` using the OTTO Engine
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
		return &compiledFunction{Function: out}, nil
	}
	return nil, fmt.Errorf("no Function")
}

// Call takes an array of results, and runs a javascript function to transform
// them into a new results
func (cfunc *compiledFunction) Call(input ...*aql.QueryResult) *aql.QueryResult {

	m := []interface{}{}
	for _, i := range input {
		s := i.GetData()
		mI := protoutil.UnWrapValue(s)
		m = append(m, mI)
	}

	//log.Printf("Inputs: %#v", m)
	//log.Printf("Function: %#v", cfunc.Function)
	/*
	 // code to deal with panics inside of the JS engine
	  defer func() {
	       if r := recover(); r != nil {
	           fmt.Println("Recovered in f", r)
	       }
	   }()
	*/
	value, err := cfunc.Function.Call(otto.Value{}, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}

	ottoVal, _ := value.Export()

	//struct_val := otto2map(ottoVal)
	log.Printf("function return: %#v", ottoVal)
	o := protoutil.WrapValue(ottoVal.(map[string]interface{}))
	return &aql.QueryResult{Result: &aql.QueryResult_Data{Data: o}}
}

// CallBool takes an array of results and evaluates them using the compiled
// javascript function, which should return a boolean
func (cfunc *compiledFunction) CallBool(input ...*aql.QueryResult) bool {

	m := []interface{}{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			mI := protoutil.AsMap(x.Edge.Data)
			m = append(m, mI)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			mI := protoutil.AsMap(x.Vertex.Data)
			m = append(m, mI)
		} else if x, ok := i.GetResult().(*aql.QueryResult_Data); ok {
			mI := protoutil.UnWrapValue(x.Data)
			m = append(m, mI)
		}
	}

	//log.Printf("Inputs: %#v", m)
	//log.Printf("Function: %#v", cfunc.Function)
	/*
	 // code to deal with panics inside of the JS engine
	  defer func() {
	       if r := recover(); r != nil {
	           fmt.Println("Recovered in f", r)
	       }
	   }()
	*/
	value, err := cfunc.Function.Call(otto.Value{}, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	ottoVal, _ := value.ToBoolean()
	return ottoVal
}

// CallValueMapBool takes a map of results and returns a boolean based on the
// evaluation of compiled javascript function
func (cfunc *compiledFunction) CallValueMapBool(input map[string]aql.QueryResult) bool {
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
	value, err := cfunc.Function.Call(otto.Value{}, c)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	ottoVal, _ := value.ToBoolean()
	return ottoVal
}

// CallValueToVertex takes a map of query results and evaluates them using
// the compiled javascript function, which should returns a series of vertex ids
func (cfunc *compiledFunction) CallValueToVertex(input map[string]aql.QueryResult) []string {
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
	value, err := cfunc.Function.Call(otto.Value{}, c)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	if value.Class() == "Array" {
		ottoVal, _ := value.Export()
		if x, ok := ottoVal.([]string); ok {
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
