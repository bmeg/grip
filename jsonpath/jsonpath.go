package jsonpath

import (
	"strings"

	"github.com/bmeg/arachne/gdbi"
	"github.com/oliveagle/jsonpath"
)

//TravelerPathLookup gets the value of a field in a Travler
func TravelerPathLookup(traveler *gdbi.Traveler, path string) interface{} {
	parts := strings.Split(path, ".")
	namespace := ""
	if len(parts) > 1 {
		namespace = strings.TrimPrefix(parts[0], "$")
		parts = parts[1:]
	}
	if namespace == "" {
		namespace = "__current__"
	}

	de := &gdbi.DataElement{}
	dmap := de.ToDict()
	if _, ok := dmap[parts[0]]; !ok {
		parts = append([]string{"data"}, parts...)
	}
	parts = append([]string{"$"}, parts...)
	field := strings.Join(parts, ".")

	var tmap map[string]interface{}
	if namespace == "__current__" {
		tmap = traveler.GetCurrent().ToDict()
	} else {
		tmap = traveler.GetMark(namespace).ToDict()
	}

	res, err := jsonpath.JsonPathLookup(tmap, field)
	if err != nil {
		return nil
	}

	return res
}

// Render takes a template and fills in the values using the data structure
func Render(template interface{}, traveler *gdbi.Traveler) interface{} {
	switch elem := template.(type) {
	case string:
		return TravelerPathLookup(traveler, elem)
	case map[string]interface{}:
		o := make(map[string]interface{}, len(elem))
		for k, v := range elem {
			o[k] = Render(v, traveler)
		}
		return o
	case []interface{}:
		o := make([]interface{}, len(elem))
		for i := range elem {
			o[i] = Render(elem[i], traveler)
		}
		return o
	default:
		return nil
	}
}
