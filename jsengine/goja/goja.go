package goja

import (
	"fmt"

	"github.com/bmeg/arachne/jsengine"
	"github.com/bmeg/arachne/jsengine/underscore"
	"github.com/dop251/goja"
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

func (gojaRun *gojaRuntime) doCall(input ...map[string]interface{}) (interface{}, error) {
	m := []goja.Value{}
	for _, i := range input {
		m = append(m, gojaRun.vm.ToValue(i))
	}
	value, err := gojaRun.call(nil, m...)
	if err != nil {
		return nil, err
	}
	val := value.Export()
	return val, nil
}

// CallDict takes an array of results, and runs a javascript function to transform
// them into a new results
func (gojaRun *gojaRuntime) CallDict(input ...map[string]interface{}) (map[string]interface{}, error) {
	o, err := gojaRun.doCall(input...)
	if err != nil {
		return nil, err
	}
	if v, ok := o.(map[string]interface{}); ok {
		return v, nil
	}
	return nil, fmt.Errorf("Bad output type")
}

// CallBool takes an array of results and evaluates them using the compiled
// javascript function, which should return a boolean
func (gojaRun *gojaRuntime) CallBool(input ...map[string]interface{}) (bool, error) {
	o, err := gojaRun.doCall(input...)
	if err != nil {
		return false, err
	}
	if v, ok := o.(bool); ok {
		return v, nil
	}
	return false, fmt.Errorf("Bad output type")
}

// CallString takes an array of results and evaluates them using the compiled
// javascript function, which should return a boolean
func (gojaRun *gojaRuntime) CallString(input ...map[string]interface{}) (string, error) {
	o, err := gojaRun.doCall(input...)
	if err != nil {
		return "", err
	}
	if v, ok := o.(string); ok {
		return v, nil
	}
	return "", fmt.Errorf("Bad output type")
}
