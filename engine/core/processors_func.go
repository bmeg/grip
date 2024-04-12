package core

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"github.com/dop251/goja"
)

// LookupVerts starts query by looking on vertices
type FlatMap struct {
	source    string
	func_name string
}

// Process LookupVerts
func (fm *FlatMap) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	vm := goja.New()
	_, err := vm.RunString(fm.source)
	if err != nil {
		log.Errorf("User function compile error: %s", err)
	}

	jobj := vm.Get(fm.func_name)
	if jobj == nil {
		log.Errorf("User Function not found: %s", fm.func_name)
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
				fout, err := jfunc(goja.Null(), dataObj)
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
