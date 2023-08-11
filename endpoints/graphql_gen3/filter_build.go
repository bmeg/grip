package main

import (
	"fmt"
	"github.com/bmeg/grip/log"

	"github.com/bmeg/grip/gripql"
)

type FilterBuilder struct {
	filter map[string]any
}

func NewFilterBuilder(i map[string]any) *FilterBuilder {
	return &FilterBuilder{i}
}

func isFilterEQ(q map[string]any) (any, bool) {
	fmt.Println("VAL OF Q IN EQ: ", q)
	return q, false

}

func isFilter(q map[string]any) (any, bool) {
	if val, ok := q["AND"]; ok {
		return val, ok

	}
	return nil, false
}

func isFilterGT(q map[string]any) (any, bool) {
	for _, i := range []string{">", "gt", "GT"} {
		if val, ok := q[i]; ok {
			return val, ok
		}
	}
	return nil, false
}

func isFilterLT(q map[string]any) (any, bool) {
	for _, i := range []string{"<", "lt", "LT"} {
		if val, ok := q[i]; ok {
			return val, ok
		}
	}
	return nil, false
}

func fieldMap(s string) string {
	if s == "id" {
		return "_gid"
	}
	return s
}

func (fb *FilterBuilder) ExtendGrip(q *gripql.Query) (*gripql.Query, error) {
	if val, ok := isFilter(fb.filter); ok {
		fmt.Println("OK?: ", val)
		if vMap, ok := val.([]map[string]any); ok {
			fmt.Println("OK2?", vMap)
			/*
				if val, ok := isFilterEQ(vMap); ok {
					fmt.Println("VAL?: ", val)
					if vMap, ok := val.(map[string]any); ok {
						fmt.Println("IN HERE?")
						for k, v := range vMap {
							k = fieldMap(k)
							q = q.Has(gripql.Eq(k, v))
						}
					}
				}*/
		}
		if val, ok := isFilterGT(fb.filter); ok {
			if vMap, ok := val.(map[string]any); ok {
				for k, v := range vMap {
					k = fieldMap(k)
					q = q.Has(gripql.Gt(k, v))
				}
			}
		}
		if val, ok := isFilterLT(fb.filter); ok {
			if vMap, ok := val.(map[string]any); ok {
				for k, v := range vMap {
					k = fieldMap(k)
					q = q.Has(gripql.Lt(k, v))
				}
			}
		}
	}
	log.Infof("Filter Query %s", q.String())
	return q, nil
}
