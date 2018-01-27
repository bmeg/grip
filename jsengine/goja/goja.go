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

type gojaRuntime struct {
	vm   *goja.Runtime
	call goja.Callable
}

var added = jsengine.AddEngine("goja", NewFunction)

// NewFunction returns a new javascript evaluator based on the `source` using the GOJA engine
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
	return &gojaRuntime{vm, f}, nil
}

// Call takes an array of results, and runs a javascript function to transform
// them into a new results
func (gojaRun *gojaRuntime) Call(input ...*aql.QueryResult) *aql.QueryResult {
	m := []goja.Value{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			mI := protoutil.AsMap(x.Edge.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			mI := protoutil.AsMap(x.Vertex.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Data); ok {
			mI := protoutil.UnWrapValue(x.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		}
	}
	value, err := gojaRun.call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	val := value.Export()
	log.Printf("function return: %#v", val)
	o := protoutil.WrapValue(val)
	return &aql.QueryResult{Result: &aql.QueryResult_Data{Data: o}}
}

// CallBool takes an array of results and evaluates them using the compiled
// javascript function, which should return a boolean
func (gojaRun *gojaRuntime) CallBool(input ...*aql.QueryResult) bool {
	m := []goja.Value{}
	for _, i := range input {
		if x, ok := i.GetResult().(*aql.QueryResult_Edge); ok {
			mI := protoutil.AsMap(x.Edge.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Vertex); ok {
			mI := protoutil.AsMap(x.Vertex.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		} else if x, ok := i.GetResult().(*aql.QueryResult_Data); ok {
			mI := protoutil.UnWrapValue(x.Data)
			m = append(m, gojaRun.vm.ToValue(mI))
		}
	}
	value, err := gojaRun.call(nil, m...)
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	gojaVal := value.ToBoolean()
	return gojaVal
}

// CallValueMapBool takes a map of results and returns a boolean based on the
// evaluation of compiled javascript function
func (gojaRun *gojaRuntime) CallValueMapBool(input map[string]aql.QueryResult) bool {
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
	value, err := gojaRun.call(nil, gojaRun.vm.ToValue(c))
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	gojaVal := value.ToBoolean()
	return gojaVal
}

// CallValueToVertex takes a map of query results and evaluates them using
// the compiled javascript function, which should returns a series of vertex ids
func (gojaRun *gojaRuntime) CallValueToVertex(input map[string]aql.QueryResult) []string {
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
	value, err := gojaRun.call(nil, gojaRun.vm.ToValue(c))
	if err != nil {
		log.Printf("Exec Error: %s", err)
	}
	//if value.Class() == "Array" {
	gojaVal := value.Export()
	if x, ok := gojaVal.([]string); ok {
		out := make([]string, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	}
	if x, ok := gojaVal.([]interface{}); ok {
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
