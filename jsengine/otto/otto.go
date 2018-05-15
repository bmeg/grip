package ottoengine

import (
	"fmt"

	"github.com/bmeg/arachne/jsengine"
	"github.com/robertkrimen/otto"
	//"github.com/robertkrimen/otto/underscore"
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
func (cfunc *compiledFunction) doCall(input ...map[string]interface{}) (otto.Value, error) {

	m := []interface{}{}
	for _, i := range input {
		m = append(m, i)
	}
	value, err := cfunc.Function.Call(otto.Value{}, m...)
	if err != nil {
		return otto.Value{}, err
	}
	return value, nil
}

// CallBool takes an array of results and evaluates them using the compiled
// javascript function, which should return a boolean
func (cfunc *compiledFunction) CallBool(input ...map[string]interface{}) (bool, error) {
	value, err := cfunc.doCall(input...)
	if err != nil {
		return false, err
	}
	out, _ := value.ToBoolean()
	return out, nil
}

func (cfunc *compiledFunction) CallString(input ...map[string]interface{}) (string, error) {
	value, err := cfunc.doCall(input...)
	if err != nil {
		return "", err
	}
	out, _ := value.ToString()
	return out, nil
}

func (cfunc *compiledFunction) CallDict(input ...map[string]interface{}) (map[string]interface{}, error) {
	value, err := cfunc.doCall(input...)
	if err != nil {
		return nil, err
	}
	ottoVal, _ := value.Export()
	if out, ok := ottoVal.(map[string]interface{}); ok {
		return out, nil
	}
	return nil, fmt.Errorf("Bad output type")
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
