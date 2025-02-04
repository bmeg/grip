package tpath

import (
	"github.com/bmeg/jsonpath"
)

func Render(template any, data map[string]any) (any, error) {
	switch elem := template.(type) {
	case string:
		path := NormalizePath(elem)
		return jsonpath.JsonPathLookup(data, path)
	case map[string]interface{}:
		o := make(map[string]interface{})
		for k, v := range elem {
			val, err := Render(v, data)
			if err == nil {
				o[k] = val
			} else if vstring, ok := v.(string); ok && vstring[0] == '$' {
				o[k] = nil
			} else {
				o[k] = v
			}
		}
		return o, nil
	case []any:
		o := make([]any, len(elem))
		for i := range elem {
			val, err := Render(elem[i], data)
			if err == nil {
				o[i] = val
			} else if vstring, ok := elem[i].(string); ok && vstring[0] == '$' {
				o[i] = nil
			} else {
				o[i] = elem[i]
			}
		}
		return o, nil
	default:
		return template, nil
	}
}
