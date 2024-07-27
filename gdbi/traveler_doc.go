package gdbi

import (
	"strings"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
)

// GetDoc returns the document representing the traveler data
func TravelerGetDoc(traveler Traveler, ns ...string) map[string]any {
	if len(ns) == 0 {
		out := map[string]any{}
		out[tpath.CURRENT] = traveler.GetCurrent().Get().ToDict()
		for _, k := range traveler.ListMarks() {
			out[k] = traveler.GetMark(k).Get().ToDict()
		}
		return out
	}
	out := map[string]any{}
	for _, n := range ns {
		if n == tpath.CURRENT {
			out[n] = traveler.GetCurrent().Get().ToDict()
		} else {
			m := traveler.GetMark(n)
			if m != nil {
				out[n] = m.Get().ToDict()
			}
		}
	}
	return out
}

// TravelerGetMarkDoc returns the document representing the traveler data
func TravelerGetMarkDoc(traveler Traveler, ns string) map[string]any {
	if ns == tpath.CURRENT {
		return traveler.GetCurrent().Get().ToDict()
	}
	m := traveler.GetMark(ns)
	if m != nil {
		return m.Get().ToDict()
	}
	return nil
}

// TravelerPathLookup gets the value of a field in the given Traveler
//
// Example for a traveler containing:
//
//	{
//	    "_current": {...},
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
	field := tpath.NormalizePath(path)
	jpath := tpath.ToLocalPath(field)
	namespace := tpath.GetNamespace(field)
	var doc map[string]any
	if namespace == tpath.CURRENT {
		doc = traveler.GetCurrent().Get().ToDict()
	} else {
		doc = traveler.GetMark(namespace).Get().ToDict()
	}
	if field == "" {
		return doc
	}
	res, err := jsonpath.JsonPathLookup(doc, jpath)
	log.Debug("field: ", field, "    jpath: ", jpath, "    namespace: ", namespace, "    doc: ", doc, "    res: ", res)

	if err != nil {
		return nil
	}
	return res
}

// TravelerSetValue(travler, "$gene.symbol.ensembl", "hi") inserts the value in the location"
func TravelerSetValue(traveler Traveler, path string, val interface{}) error {
	field := tpath.NormalizePath(path)
	namespace := tpath.GetNamespace(field)
	jpath := tpath.ToLocalPath(field)
	if field == "" {
		return nil
	}
	doc := TravelerGetMarkDoc(traveler, namespace)
	err := jsonpath.JsonPathSet(doc, jpath, val)
	if err != nil {
		return err
	}
	r := DataElement{}
	r.FromDict(doc)
	traveler.UpdateMark(namespace, &r)
	return nil
}

/*
func TravelerSetMarkDoc(traveler Traveler, ns string, doc map[string]any ) error {

	d = DataElement{}


	if ns == tpath.CURRENT {
		return traveler.GetCurrent().Get().ToDict()
	}
	m := traveler.GetMark(ns)
	if m != nil {
		return m.Get().ToDict()
	}
	return nil
}
*/

// TravelerPathExists returns true if the field exists in the given Traveler
func TravelerPathExists(traveler Traveler, path string) bool {
	field := tpath.NormalizePath(path)
	jpath := tpath.ToLocalPath(field)
	namespace := tpath.GetNamespace(field)
	if jpath == "" {
		return false
	}
	doc := TravelerGetMarkDoc(traveler, namespace)
	_, err := jsonpath.JsonPathLookup(doc, jpath)
	return err == nil
}

// RenderTraveler takes a template and fills in the values using the data structure
func RenderTraveler(traveler Traveler, template interface{}) interface{} {
	doc := TravelerGetDoc(traveler)
	out, _ := tpath.Render(template, doc)
	return out
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
		namespace := tpath.GetNamespace(key)
		switch namespace {
		case tpath.CURRENT:
			// noop
		default:
			log.Errorf("SelectTravelerFields: only can select field from current traveler")
			continue KeyLoop
		}
		path := tpath.NormalizePath(key)
		jpath := tpath.ToLocalPath(path)
		spath := strings.TrimPrefix(jpath, "$.")
		if exclude {
			excludePaths = append(excludePaths, spath)
		} else {
			includePaths = append(includePaths, spath)
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
		case "_gid", "_label", "_from", "_to":
			// noop
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
		case "_gid":
			result.ID = ""
		case "_label":
			result.Label = ""
		case "_from":
			result.From = ""
		case "_to":
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
