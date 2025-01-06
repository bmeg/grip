package core

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"github.com/dop251/goja"
)

// LookupVerts starts query by looking on vertices
type FlatMap struct {
	Source    string
	Func_name string
	Args      map[string]any
}

// Process LookupVerts
func (fm *FlatMap) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	vm := goja.New()

	// Add support for console.log for debugging purposes
	console := vm.NewObject()
	_ = console.Set("log", func(call goja.FunctionCall) goja.Value {
		args := make([]any, len(call.Arguments))
		for i, arg := range call.Arguments {
			args[i] = arg.Export()
		}
		log.Infof("JS log: %v", args...)
		return goja.Undefined()
	})
	_ = vm.Set("console", console)

	_, err := vm.RunString(fm.Source)
	if err != nil {
		log.Errorf("User function compile error: %s", err)
	}

	jobj := vm.Get(fm.Func_name)
	if jobj == nil {
		log.Errorf("User Function not found: %s", fm.Func_name)
	}

	jfunc, ok := goja.AssertFunction(jobj)
	if !ok {
		log.Errorf("Defined object not function: %#v", jobj)
	}
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if jfunc != nil {
				src := t.GetCurrent().Get()
				data := src.ToDict()
				dataObj := vm.ToValue(data)

				argsObj := vm.ToValue(fm.Args)

				fout, err := jfunc(goja.Null(), dataObj, argsObj)

				if err == nil {
					o := fout.Export()
					if oList, ok := o.([]any); ok {
						for _, od := range oList {
							if om, ok := od.(map[string]any); ok {
								d := gdbi.DataElement{}
								d.FromDict(om)
								d.ID = src.ID
								d.Label = src.Label
								out <- t.AddCurrent(&d)
							}
						}
					}
				} else {
					log.Errorf("Function error: %s", err)
				}
			}
		}
	}()
	return ctx
}
