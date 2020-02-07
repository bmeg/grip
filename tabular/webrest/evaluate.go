
package webrest

import (
  "log"
  _ "github.com/go-python/gpython/builtin"
  "github.com/go-python/gpython/py"
  "github.com/go-python/gpython/vm"
  "github.com/go-python/gpython/compile"
)


func PyObject(i interface{}) py.Object {
  if xMap, ok := i.(map[string]interface{}); ok {
    o := py.StringDict{}
    for k, v := range xMap {
      o[k] = PyObject(v)
    }
    return o
  } else if xList, ok := i.([]interface{}); ok {
      o := py.NewList()
      for _, v := range xList {
        o.Append( PyObject(v) )
      }
      return o
  } else if xString, ok := i.(string); ok {
    return py.String(xString)
  } else if xInt, ok := i.(int); ok {
    return py.Int(xInt)
  } else if xInt, ok := i.(int64); ok {
    return py.Int(xInt)
  } else if xFloat, ok := i.(float64); ok {
    return py.Float(xFloat)
  } else if xBool, ok := i.(bool); ok {
    return py.Bool(xBool)
  }
  return nil
}

func FromPyObject(i py.Object) interface{} {
  if xMap, ok := i.(py.StringDict); ok {
    out := map[string]interface{}{}
    for k, v := range xMap {
      out[k] = FromPyObject(v)
    }
    return out
  } else if xList, ok := i.(*py.List); ok {
    out := []interface{}{}
    for _, v := range xList.Items {
      out = append(out, FromPyObject(v))
    }
    return out
  } else if xStr, ok := i.(py.String); ok {
    return string(xStr)
  } else if xFloat, ok := i.(py.Float); ok {
    return float64(xFloat)
  } else if xBool, ok := i.(py.Bool); ok {
    return bool(xBool)
  } else if xInt, ok := i.(py.Int); ok {
    return int64(xInt)
  }
  return nil
}

type PyCode struct {
  module *py.Module
}

func PyCompile(codeStr string) (*PyCode, error){

    obj, err := compile.Compile(codeStr, "test.py", "exec", 0, true)
    if err != nil {
      log.Fatalf("Can't compile %q: %v", codeStr, err)
    }

    code := obj.(*py.Code)
    //log.Printf("Code: %s", code)
    module := py.NewModule("__main__", "", nil, nil)
  	//res, err := vm.EvalCode(code, module.Globals, module.Globals)
    _, err = vm.Run(module.Globals, module.Globals, code, nil)
    if err != nil {
  		py.TracebackDump(err)
  		log.Fatal(err)
      return nil, err
  	}
    //log.Printf("Module: %s", module.Globals)
    return &PyCode{module:module}, nil
}

func (p *PyCode) Evaluate(method string, inputs... map[string]interface{}) map[string]interface{} {
  fun := p.module.Globals[method]
  in := py.Tuple{}
  for _, i := range inputs {
    data := PyObject(i)
    in = append(in, data)
  }
  out, err := py.Call(fun, in, nil)
  if err != nil {
		py.TracebackDump(err)
		log.Printf("Map Error: %s", err)
    return nil
	}
  o := FromPyObject(out)
  if out, ok := o.(map[string]interface{}); ok {
    return out
  }
  log.Printf("Incorrect return type: %s", out)
  return nil
}
