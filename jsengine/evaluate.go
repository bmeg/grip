package jsengine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"log"
)

type JSEngine interface {
	Call(input ...*aql.QueryResult) *aql.QueryResult
	CallBool(input ...*aql.QueryResult) bool
	CallValueMapBool(input map[string]aql.QueryResult) bool
	CallValueToVertex(input map[string]aql.QueryResult) []string
}

type genfunc func(string, []string) (JSEngine, error)

var engines map[string]genfunc = make(map[string]genfunc)

func AddEngine(name string, gen genfunc) bool {
	engines[name] = gen
	return true
}

func NewJSEngine(code string, imports []string) (JSEngine, error) {
	if x, ok := engines["v8"]; ok {
		return x(code, imports)
	}
	if x, ok := engines["goja"]; ok {
		return x(code, imports)
	}
	if x, ok := engines["otto"]; ok {
		return x(code, imports)
	}
	return nil, fmt.Errorf("Javascript Engine not found")
}
