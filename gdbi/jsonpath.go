package gdbi

import (
	// "fmt"

	"strings"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/travelerpath"
	"github.com/bmeg/jsonpath"
)

// GetDoc returns the document referenced by the provided namespace.
//
// Example for a traveler containing:
//
//	{
//	    "current": {...},
//	    "marks": {
//	      "gene": {
//	        "gid": 1,
//	        "label": "gene",
//	        "data": {
//	          "symbol": {
//	            "ensembl": "ENSG00000012048",
//	            "hgnc": 1100,
//	            "entrez": 672
//	          }
//	        }
//	      }
//	    }
//	  }
//	}
//
// GetDoc(traveler, "gene") returns:
//
//	{
//	  "gid": 1,
//	  "label": "gene",
//	  "data": {
//	    "symbol": {
//	      "ensembl": "ENSG00000012048",
//	      "hgnc": 1100,
//	      "entrez": 672
//	    }
//	  }
//	}
func GetDoc(traveler Traveler, namespace string) map[string]interface{} {
	var tmap map[string]interface{}
	if namespace == travelerpath.Current {
		dr := traveler.GetCurrent()
		if dr == nil {
			return nil
		}
		tmap = dr.Get().ToDict()
	} else {
		dr := traveler.GetMark(namespace)
		if dr == nil {
			return nil
		}
		tmap = dr.Get().ToDict()
	}
	return tmap
}

// TravelerPathLookup gets the value of a field in the given Traveler
//
// Example for a traveler containing:
//
//	{
//	    "current": {...},
//	    "marks": {
//	      "gene": {
//	        "gid": 1,
//	        "label": "gene",
//	        "data": {
//	          "symbol": {
//	            "ensembl": "ENSG00000012048",
//	            "hgnc": 1100,
//	            "entrez": 672
//	          }
//	        }
//	      }
//	    }
//	  }
//	}
//
// TravelerPathLookup(travler, "$gene.symbol.ensembl") returns "ENSG00000012048"
func TravelerPathLookup(traveler Traveler, path string) interface{} {
	namespace := travelerpath.GetNamespace(path)
	field := travelerpath.GetJSONPath(path)
	doc := GetDoc(traveler, namespace)
	if field == "" {
		//fmt.Printf("Null field, return %#v\n", doc)
		return doc
	}
	res, err := jsonpath.JsonPathLookup(doc, field)
	if err != nil {
		return nil
	}
	return res
}

// TravelerSetValue(travler, "$gene.symbol.ensembl", "hi") inserts the value in the location"
func TravelerSetValue(traveler Traveler, path string, val interface{}) error {
	namespace := travelerpath.GetNamespace(path)
	field := travelerpath.GetJSONPath(path)
	if field == "" {
		return nil
	}
	doc := GetDoc(traveler, namespace)
	return jsonpath.JsonPathSet(doc, field, val)
}

// TravelerPathExists returns true if the field exists in the given Traveler
func TravelerPathExists(traveler Traveler, path string) bool {
	namespace := travelerpath.GetNamespace(path)
	field := travelerpath.GetJSONPath(path)
	if field == "" {
		return false
	}
	doc := GetDoc(traveler, namespace)
	_, err := jsonpath.JsonPathLookup(doc, field)
	return err == nil
}

// RenderTraveler takes a template and fills in the values using the data structure
func RenderTraveler(traveler Traveler, template interface{}) interface{} {
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
func SelectTravelerFields(t Traveler, keys ...string) Traveler {
	includePaths := []string{}
	excludePaths := []string{}
KeyLoop:
	for _, key := range keys {
		exclude := false
		if strings.HasPrefix(key, "-") {
			exclude = true
			key = strings.TrimPrefix(key, "-")
		}
		namespace := travelerpath.GetNamespace(key)
		switch namespace {
		case travelerpath.Current:
			// noop
		default:
			log.Errorf("SelectTravelerFields: only can select field from current traveler")
			continue KeyLoop
		}
		path := travelerpath.GetJSONPath(key)
		path = strings.TrimPrefix(path, "$.")

		if exclude {
			excludePaths = append(excludePaths, path)
		} else {
			includePaths = append(includePaths, path)
		}
	}

	var out Traveler = &BaseTraveler{}
	out = out.AddCurrent(&DataElement{
		Data: map[string]interface{}{},
	})
	for _, mark := range t.ListMarks() {
		out = out.AddMark(mark, t.GetMark(mark))
	}

	var cde *DataElement
	var ode *DataElement

	cde = t.GetCurrent().Get()
	ode = out.GetCurrent().Get()

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
	ode.Loaded = true
	return out
}

func includeFields(new, old *DataElement, paths []string) *DataElement {
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

func excludeFields(elem *DataElement, paths []string) *DataElement {
	result := &DataElement{
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
