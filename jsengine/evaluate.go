package jsengine

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
)

// JSEngine is the common JavaScript engine interface
type JSEngine interface {
	Call(input ...*aql.QueryResult) *aql.QueryResult
	CallBool(input ...*aql.QueryResult) bool
	CallValueMapBool(input map[string]aql.QueryResult) bool
	CallValueToVertex(input map[string]aql.QueryResult) []string
}

type genfunc func(string, []string) (JSEngine, error)

var engines = make(map[string]genfunc)

// AddEngine adds JavaScript engine to common registry
// Because some of the JS engines, like the V8 engine,
// are optionally built with compile tags, this tracks
// which drivers are actually avalible
func AddEngine(name string, gen genfunc) bool {
	engines[name] = gen
	return true
}

// NewJSEngine creates a new JavaScript engine
// using the 'best' driver (v8 in avalible). Its compiles
// `code` and allows for multiple dependency imports (like underscore)
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
