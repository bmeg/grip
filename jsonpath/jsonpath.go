package jsonpath

import (
	"fmt"
	"strings"

	"github.com/bmeg/arachne/gdbi"
	"github.com/oliveagle/jsonpath"
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
// GetJSONPath("$gene.symbol.ensembl") returns "$.data.symbol.ensembl"
func GetJSONPath(path string) string {
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		parts = parts[1:]
	}

	de := &gdbi.DataElement{}
	dmap := de.ToDict()
	if _, ok := dmap[parts[0]]; !ok {
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

// TravelerPathLookup gets the value of a field in a Travler
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
	doc := GetDoc(traveler, namespace)

	res, err := jsonpath.JsonPathLookup(doc, field)
	if err != nil {
		return nil
	}

	return res
}

// RenderTraveler takes a template and fills in the values using the data structure
func RenderTraveler(traveler *gdbi.Traveler, template interface{}) interface{} {
	switch elem := template.(type) {
	case string:
		return TravelerPathLookup(traveler, elem)
	case map[string]interface{}:
		o := make(map[string]interface{}, len(elem))
		for k, v := range elem {
			o[k] = RenderTraveler(traveler, v)
		}
		return o
	case []interface{}:
		o := make([]interface{}, len(elem))
		for i := range elem {
			o[i] = RenderTraveler(traveler, elem[i])
		}
		return o
	default:
		return nil
	}
}

// SelectTravelerFields returns a new copy of the traveler with only the selected fields
func SelectTravelerFields(t *gdbi.Traveler, keys ...string) (*gdbi.Traveler, error) {
	out := &gdbi.Traveler{}
	out = out.AddCurrent(&gdbi.DataElement{
		Data: map[string]interface{}{},
	})

	for _, key := range keys {
		namespace := GetNamespace(key)
		path := GetJSONPath(key)
		path = strings.TrimPrefix(path, "$.")

		var cde *gdbi.DataElement
		var ode *gdbi.DataElement
		switch namespace {
		case Current:
			cde = t.GetCurrent()
			ode = out.GetCurrent()
		default:
			cde = t.GetMark(namespace)
			ode = out.GetMark(namespace)
			if ode == nil {
				out = out.AddMark(namespace, &gdbi.DataElement{
					Data: map[string]interface{}{},
				})
				ode = out.GetMark(namespace)
			}
		}

		switch path {
		case "gid":
			ode.ID = cde.ID
		case "label":
			ode.Label = cde.Label
		case "from":
			ode.From = cde.From
		case "to":
			ode.To = cde.To
		case "data":
			ode.Data = cde.Data
		default:
			parts := strings.Split(path, ".")
			var data map[string]interface{}
			var ok bool
			data = cde.Data
			for i := 0; i < len(parts); i++ {
				if parts[i] == "data" {
					continue
				}
				if i == len(parts)-1 {
					ode.Data[parts[i]] = data[parts[i]]
				} else {
					ode.Data[parts[i]] = map[string]interface{}{}
					data, ok = data[parts[i]].(map[string]interface{})
					if !ok {
						return nil, fmt.Errorf("something went wrong when selecting fields on the traveler to return")
					}
				}
			}
		}
	}
	return out, nil
}
