package gdbi

import (
	"strings"
	"sync"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonpath"
	"github.com/bytedance/sonic"
)

type travelerPathPlan struct {
	namespace  string
	jpath      string
	directPath []any
	hasDirect  bool
}

var travelerPathPlanCache sync.Map

func rowToDictOrEmpty(row Row) map[string]any {
	if row == nil {
		return map[string]any{}
	}
	return row.ToDict()
}

func resolveTravelerPathPlan(path string) travelerPathPlan {
	if cached, ok := travelerPathPlanCache.Load(path); ok {
		if plan, ok := cached.(travelerPathPlan); ok {
			return plan
		}
	}
	field := tpath.NormalizePath(path)
	jpath := tpath.ToLocalPath(field)
	plan := travelerPathPlan{
		namespace: tpath.GetNamespace(field),
		jpath:     jpath,
	}
	if spath, ok := tpath.ParseDirectPath(jpath); ok {
		plan.directPath = spath
		plan.hasDirect = true
	}
	travelerPathPlanCache.Store(path, plan)
	return plan
}

func rowSystemField(row Row, key string) (any, bool) {
	switch key {
	case "_id":
		return row.GetID(), true
	case "_label":
		return row.GetLabel(), true
	case "_from":
		return row.GetFrom(), true
	case "_to":
		return row.GetTo(), true
	default:
		return nil, false
	}
}

func lookupDirectPathValue(root any, path []any) (any, bool) {
	candidates := []any{root}
	for _, seg := range path {
		next := make([]any, 0, len(candidates))
		switch s := seg.(type) {
		case tpath.WildcardToken:
			for _, cand := range candidates {
				next = appendWildcardCandidates(next, cand)
			}
		case string:
			for _, cand := range candidates {
				next = appendStringCandidates(next, cand, s)
			}
		case int:
			for _, cand := range candidates {
				next = appendIndexCandidates(next, cand, s)
			}
		default:
			return nil, false
		}
		if len(next) == 0 {
			return nil, false
		}
		candidates = next
	}
	if len(candidates) == 1 {
		return candidates[0], true
	}
	return candidates, true
}

func appendWildcardCandidates(out []any, val any) []any {
	switch cv := val.(type) {
	case []any:
		out = append(out, cv...)
	case []map[string]any:
		for _, item := range cv {
			out = append(out, item)
		}
	case map[string]any:
		for _, item := range cv {
			out = append(out, item)
		}
	}
	return out
}

func appendStringCandidates(out []any, val any, key string) []any {
	switch cv := val.(type) {
	case map[string]any:
		if v, ok := cv[key]; ok {
			out = append(out, v)
		}
	case []any:
		for _, item := range cv {
			out = appendStringCandidates(out, item, key)
		}
	case []map[string]any:
		for _, m := range cv {
			if v, ok := m[key]; ok {
				out = append(out, v)
			}
		}
	}
	return out
}

func appendIndexCandidates(out []any, val any, idx int) []any {
	switch arr := val.(type) {
	case []any:
		if idx >= 0 && idx < len(arr) {
			out = append(out, arr[idx])
		}
	case []map[string]any:
		if idx >= 0 && idx < len(arr) {
			out = append(out, arr[idx])
		}
	}
	return out
}

func directPathHasWildcard(path []any) bool {
	for _, seg := range path {
		if _, ok := seg.(tpath.WildcardToken); ok {
			return true
		}
	}
	return false
}

// GetDoc returns the document representing the traveler data
func TravelerGetDoc(traveler Traveler, ns ...string) map[string]any {
	if traveler == nil {
		return map[string]any{}
	}
	if len(ns) == 0 {
		out := map[string]any{}
		out[tpath.CURRENT] = rowToDictOrEmpty(traveler.GetCurrent())
		for _, k := range traveler.ListMarks() {
			if mark := traveler.GetMark(k); mark != nil {
				out[k] = mark.ToDict()
			} else {
				out[k] = map[string]any{}
			}
		}
		return out
	}
	out := map[string]any{}
	for _, n := range ns {
		if n == tpath.CURRENT {
			out[n] = rowToDictOrEmpty(traveler.GetCurrent())
		} else {
			m := traveler.GetMark(n)
			if m != nil {
				out[n] = m.ToDict()
			} else {
				out[n] = map[string]any{}
			}
		}
	}
	return out
}

// TravelerGetMarkDoc returns the document representing the traveler data
func TravelerGetMarkDoc(traveler Traveler, ns string) map[string]any {
	if traveler == nil {
		return map[string]any{}
	}
	if ns == tpath.CURRENT {
		return rowToDictOrEmpty(traveler.GetCurrent())
	}
	m := traveler.GetMark(ns)
	if m != nil {
		return m.ToDict()
	}
	return map[string]any{}
}

// TravelerPathLookup gets the value of a field in the given Traveler
func TravelerPathLookup(traveler Traveler, path string) interface{} {
	plan := resolveTravelerPathPlan(path)

	var row Row
	if plan.namespace == tpath.CURRENT {
		row = traveler.GetCurrent()
	} else {
		row = traveler.GetMark(plan.namespace)
	}

	if row == nil {
		return nil
	}

	if plan.hasDirect {
		spath := plan.directPath
		hasWildcard := directPathHasWildcard(spath)
		if len(spath) > 0 {
			if first, ok := spath[0].(string); ok && strings.HasPrefix(first, "_") {
				if len(spath) == 1 {
					if v, ok := rowSystemField(row, first); ok {
						return v
					}
				}
				return nil
			}
		}
		if !hasWildcard && !row.IsLoaded() {
			raw := row.GetRaw()
			if raw != "" {
				node, err := sonic.GetFromString(raw, spath...)
				if err == nil {
					v, ierr := node.Interface()
					if ierr == nil {
						return v
					}
				}
				// Direct paths should not force full map materialization.
				return nil
			}
		}
		if v, ok := lookupDirectPathValue(row.GetPayload(), spath); ok {
			return v
		}
		return nil
	}

	doc := row.ToDict()
	if plan.jpath == "" {
		return doc
	}
	res, err := jsonpath.JsonPathLookup(doc, plan.jpath)
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
	if traveler == nil {
		return false
	}
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
	out, _ := renderTravelerValue(traveler, template)
	return out
}

func renderTravelerValue(traveler Traveler, template any) (any, bool) {
	switch elem := template.(type) {
	case string:
		if v := TravelerPathLookup(traveler, elem); v != nil {
			return v, true
		}
		if TravelerPathExists(traveler, elem) {
			return nil, true
		}
		if strings.HasPrefix(elem, "$") {
			return nil, true
		}
		return elem, false
	case map[string]any:
		o := make(map[string]any, len(elem))
		for k, v := range elem {
			if rv, ok := renderTravelerValue(traveler, v); ok {
				o[k] = rv
			} else {
				o[k] = v
			}
		}
		return o, true
	case []any:
		o := make([]any, len(elem))
		for i := range elem {
			if rv, ok := renderTravelerValue(traveler, elem[i]); ok {
				o[i] = rv
			} else {
				o[i] = elem[i]
			}
		}
		return o, true
	default:
		return template, true
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
		namespace := tpath.GetNamespace(key)
		if namespace != tpath.CURRENT {
			log.Errorf("SelectTravelerFields: only can select field from current traveler")
			continue KeyLoop
		}
		path := tpath.NormalizePath(key)
		jpath := tpath.ToLocalPath(path)
		spath := strings.TrimPrefix(jpath, "$.")
		// Standardize spath: remove 'data.' or '_data.' if present
		spath = strings.TrimPrefix(spath, "data.")
		spath = strings.TrimPrefix(spath, "_data.")
		if spath == "data" || spath == "_data" {
			// selecting entire data map
			spath = ""
		}

		if exclude {
			excludePaths = append(excludePaths, spath)
		} else {
			includePaths = append(includePaths, spath)
		}
	}

	curr := t.GetCurrent()
	ode := &DataElement{
		ID:    curr.GetID(),
		Label: curr.GetLabel(),
		From:  curr.GetFrom(),
		To:    curr.GetTo(),
		Data:  map[string]interface{}{},
	}

	cde := curr.ToDict()

	if len(excludePaths) > 0 {
		cde = excludeFields(cde, excludePaths)
	}

	if len(includePaths) > 0 {
		cde = includeFields(cde, includePaths)
	}

	if len(keys) != 0 {
		for k, v := range cde {
			switch k {
			case "_id":
				ode.ID = v.(string)
			case "_label":
				ode.Label = v.(string)
			case "_from":
				ode.From = v.(string)
			case "_to":
				ode.To = v.(string)
			default:
				ode.Data[k] = v
			}
		}
	}
	ode.Loaded = true
	var out Traveler = t.Copy()
	out = out.AddCurrent(ode)
	return out
}

func includeFields(old map[string]any, paths []string) map[string]any {
	newData := make(map[string]any)
Include:
	for _, path := range paths {
		path = strings.TrimPrefix(path, "data.")
		path = strings.TrimPrefix(path, "_data.")
		switch path {
		case "_id", "_label", "_from", "_to", "":
			// already handled or whole record
			if path == "" {
				for k, v := range old {
					newData[k] = v
				}
			} else {
				if val, ok := old[path]; ok {
					newData[path] = val
				}
			}
		default:
			parts := strings.Split(path, ".")
			var data map[string]interface{}
			data = old
			for i := 0; i < len(parts); i++ {
				key := parts[i]
				if i == len(parts)-1 {
					if val, ok := data[key]; ok {
						newData[key] = val
					} else {
						log.Errorf("includeFields: property does not exist: %s", path)
						continue Include
					}
				} else {
					if val, ok := data[key]; !ok {
						log.Errorf("includeFields: property does not exist: %s", path)
						continue Include
					} else {
						if next, ok := val.(map[string]interface{}); ok {
							data = next
						} else {
							log.Errorf("includeFields: property is not a map: %s", parts[i])
							continue Include
						}
					}
				}
			}
		}
	}
	return newData
}

func excludeFields(old map[string]any, paths []string) map[string]any {
	result := make(map[string]any)
	for k, v := range old {
		result[k] = v
	}
Exclude:
	for _, path := range paths {
		path = strings.TrimPrefix(path, "data.")
		path = strings.TrimPrefix(path, "_data.")
		switch path {
		case "_id", "_label", "_from", "_to", "":
			delete(result, path)
		default:
			parts := strings.Split(path, ".")
			data := result
			for i := 0; i < len(parts); i++ {
				key := parts[i]
				if i == len(parts)-1 {
					delete(data, key)
				} else {
					if val, ok := data[key]; ok {
						if next, ok := val.(map[string]interface{}); ok {
							data = next
						} else {
							log.Errorf("excludeFields: property is not a map: %s", key)
							continue Exclude
						}
					} else {
						log.Errorf("excludeFields: property does not exist: %s", path)
						continue Exclude
					}
				}
			}
		}
	}
	return result
}
