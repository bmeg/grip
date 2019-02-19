package jsonpath

import (
	// "fmt"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/oliveagle/jsonpath"
	log "github.com/sirupsen/logrus"
)

// Current represents the 'current' traveler namespace
var Current = "__current__"

// GetNamespace returns the namespace of the provided path
//
// Example:
// GetNamespace("$gene.symbol.ensembl") returns "gene"
func GetNamespace(path string) string {
	namespace := ""
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		namespace = strings.TrimPrefix(parts[0], "$")
	}
	if namespace == "" {
		namespace = Current
	}
	return namespace
}

// GetJSONPath strips the namespace from the path and returns the valid
// Json path within the document referenced by the namespace
//
// Example:
// GetJSONPath("gene.symbol.ensembl") returns "$.data.symbol.ensembl"
func GetJSONPath(path string) string {
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return ""
	}
	found := false
	for _, v := range gripql.ReservedFields {
		if parts[0] == v {
			found = true
			parts[0] = strings.TrimPrefix(parts[0], "_")
		}
	}

	if !found {
		parts = append([]string{"data"}, parts...)
	}

	parts = append([]string{"$"}, parts...)
	return strings.Join(parts, ".")
}

// GetDoc returns the document referenced by the provided namespace.
//
// Example for a traveler containing:
// {
//     "current": {...},
//     "marks": {
//       "gene": {
//         "gid": 1,
//         "label": "gene",
//         "data": {
//           "symbol": {
//             "ensembl": "ENSG00000012048",
//             "hgnc": 1100,
//             "entrez": 672
//           }
//         }
//       }
//     }
//   }
// }
//
// GetDoc(traveler, "gene") returns:
//
// {
//   "gid": 1,
//   "label": "gene",
//   "data": {
//     "symbol": {
//       "ensembl": "ENSG00000012048",
//       "hgnc": 1100,
//       "entrez": 672
//     }
//   }
// }
func GetDoc(traveler *gdbi.Traveler, namespace string) map[string]interface{} {
	var tmap map[string]interface{}
	if namespace == Current {
		tmap = traveler.GetCurrent().ToDict()
	} else {
		tmap = traveler.GetMark(namespace).ToDict()
	}
	return tmap
}

// TravelerPathLookup gets the value of a field in the given Traveler
//
// Example for a traveler containing:
// {
//     "current": {...},
//     "marks": {
//       "gene": {
//         "gid": 1,
//         "label": "gene",
//         "data": {
//           "symbol": {
//             "ensembl": "ENSG00000012048",
//             "hgnc": 1100,
//             "entrez": 672
//           }
//         }
//       }
//     }
//   }
// }
//
// TravelerPathLookup(travler, "$gene.symbol.ensembl") returns "ENSG00000012048"
func TravelerPathLookup(traveler *gdbi.Traveler, path string) interface{} {
	namespace := GetNamespace(path)
	field := GetJSONPath(path)
	if field == "" {
		return nil
	}
	doc := GetDoc(traveler, namespace)
	res, err := jsonpath.JsonPathLookup(doc, field)
	if err != nil {
		return nil
	}
	return res
}

// TravelerPathExists returns true if the field exists in the given Traveler
func TravelerPathExists(traveler *gdbi.Traveler, path string) bool {
	namespace := GetNamespace(path)
	field := GetJSONPath(path)
	if field == "" {
		return false
	}
	doc := GetDoc(traveler, namespace)
	_, err := jsonpath.JsonPathLookup(doc, field)
	if err != nil {
		return false
	}
	return true
}

// RenderTraveler takes a template and fills in the values using the data structure
func RenderTraveler(traveler *gdbi.Traveler, template interface{}) interface{} {
	switch elem := template.(type) {
	case string:
		return TravelerPathLookup(traveler, elem)
	case map[string]interface{}:
		o := make(map[string]interface{})
		for k, v := range elem {
			val := RenderTraveler(traveler, v)
			o[k] = val
		}
		return o
	case []interface{}:
		o := make([]interface{}, len(elem))
		for i := range elem {
			val := RenderTraveler(traveler, elem[i])
			o[i] = val
		}
		return o
	default:
		return nil
	}
}

// SelectTravelerFields returns a new copy of the traveler with only the selected fields
func SelectTravelerFields(t *gdbi.Traveler, keys ...string) *gdbi.Traveler {
	includePaths := []string{}
	excludePaths := []string{}
KeyLoop:
	for _, key := range keys {
		exclude := false
		if strings.HasPrefix(key, "-") {
			exclude = true
			key = strings.TrimPrefix(key, "-")
		}
		namespace := GetNamespace(key)
		switch namespace {
		case Current:
			// noop
		default:
			log.Errorf("SelectTravelerFields: only can select field from current traveler")
			continue KeyLoop
		}
		path := GetJSONPath(key)
		path = strings.TrimPrefix(path, "$.")

		if exclude {
			excludePaths = append(excludePaths, path)
		} else {
			includePaths = append(includePaths, path)
		}
	}

	out := &gdbi.Traveler{}
	out = out.AddCurrent(&gdbi.DataElement{
		Data: map[string]interface{}{},
	})
	for _, mark := range t.ListMarks() {
		out = out.AddMark(mark, t.GetMark(mark))
	}

	var cde *gdbi.DataElement
	var ode *gdbi.DataElement

	cde = t.GetCurrent()
	ode = out.GetCurrent()

	if len(excludePaths) > 0 {
		cde = excludeFields(cde, excludePaths)
		for k, v := range cde.Data {
			ode.Data[k] = v
		}
	}

	ode.ID = cde.ID
	ode.Label = cde.Label
	ode.From = cde.From
	ode.To = cde.To

	if len(includePaths) > 0 {
		ode = includeFields(ode, cde, includePaths)
	}

	return out
}

func includeFields(new, old *gdbi.DataElement, paths []string) *gdbi.DataElement {
	newData := make(map[string]interface{})
Include:
	for _, path := range paths {
		switch path {
		case "gid", "label", "from", "to":
			// noop
		case "data":
			for k, v := range old.Data {
				newData[k] = v
			}
		default:
			parts := strings.Split(path, ".")
			var data map[string]interface{}
			var ok bool
			data = old.Data
			for i := 0; i < len(parts); i++ {
				if parts[i] == "data" {
					continue
				}
				if i == len(parts)-1 {
					if val, ok := data[parts[i]]; ok {
						newData[parts[i]] = val
					} else {
						log.Errorf("SelectTravelerFields: includeFields: property does not exist: %s", path)
						continue Include
					}
				} else {
					if _, ok := data[parts[i]]; !ok {
						log.Errorf("SelectTravelerFields: includeFields: property does not exist: %s", path)
						continue Include
					}
					newData[parts[i]] = map[string]interface{}{}
					data, ok = data[parts[i]].(map[string]interface{})
					if !ok {
						log.Errorf("SelectTravelerFields: includeFields: property does not exist: %s", path)
						continue Include
					}
				}
			}
		}
	}
	new.Data = newData
	return new
}

func excludeFields(elem *gdbi.DataElement, paths []string) *gdbi.DataElement {
	result := &gdbi.DataElement{
		ID:    elem.ID,
		Label: elem.Label,
		From:  elem.From,
		To:    elem.To,
		Data:  map[string]interface{}{},
	}
	for k, v := range elem.Data {
		result.Data[k] = v
	}
	data := result.Data
Exclude:
	for _, path := range paths {
		switch path {
		case "gid":
			result.ID = ""
		case "label":
			result.Label = ""
		case "from":
			result.From = ""
		case "to":
			result.To = ""
		case "data":
			result.Data = map[string]interface{}{}
		default:
			parts := strings.Split(path, ".")
			for i := 0; i < len(parts); i++ {
				if parts[i] == "data" {
					continue
				}
				if i == len(parts)-1 {
					if _, ok := data[parts[i]]; !ok {
						log.Errorf("SelectTravelerFields: excludeFields: property does not exist: %s", path)
						continue Exclude
					}
					delete(data, parts[i])
				} else {
					var ok bool
					var val interface{}
					var mapVal map[string]interface{}
					if val, ok = elem.Data[parts[i]]; ok {
						if mapVal, ok = val.(map[string]interface{}); ok {
							data[parts[i]] = mapVal
						}
					}
					if !ok {
						log.Errorf("SelectTravelerFields: excludeFields: property does not exist: %s", path)
						continue Exclude
					}
				}
			}
		}
	}
	return result
}
