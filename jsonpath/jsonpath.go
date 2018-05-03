package jsonpath

import (
	"strings"

	"github.com/bmeg/arachne/gdbi"
	"github.com/oliveagle/jsonpath"
)

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
		namespace = "__current__"
	}
	return namespace
}

// GetJsonPath strips the namespace from the path and returns the valid
// Json path within the document referenced by the namespace
//
// Example:
// GetJsonPath("$gene.symbol.ensembl") returns "$.data.symbol.ensembl"
func GetJsonPath(path string) string {
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
	if namespace == "__current__" {
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
	field := GetJsonPath(path)
	doc := GetDoc(traveler, namespace)

	res, err := jsonpath.JsonPathLookup(doc, field)
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
