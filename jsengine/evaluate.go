package jsengine

import (
	"fmt"
	"github.com/bmeg/arachne/ophion"
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

func (self *CompiledFunction) Call(input ...*ophion.QueryResult) *ophion.QueryResult {

	m := []interface{}{}
	for _, i := range input {
		s := i.GetStruct()
		m_i := protoutil.AsMap(s)
		m = append(m, m_i)
	}

	log.Printf("Inputs: %#v", m)
	log.Printf("Function: %#v", self.Function)
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
	return &ophion.QueryResult{&ophion.QueryResult_Struct{o}}
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
