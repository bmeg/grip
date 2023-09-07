package main

import (
	"fmt"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

type FilterBuilder struct {
	filter map[string]any
}

func NewFilterBuilder(i map[string]any) *FilterBuilder {
	return &FilterBuilder{i}
}

func isFilterEQ(q map[string]any) (any, bool) {
	if val, ok := q["IN"]; ok {
		return val, ok
	}
	return q, false
}

func isFilter(q map[string]any) (any, bool) {
	// this first and doesn't seem to suit any purpose
	// but this could be for multifaceted queries which aren't currently supported
	if val, ok := q["AND"]; ok {
		return val, ok
	}
	return nil, false
}

func isFilterGT(q map[string]any) (any, bool) {
	for _, i := range []string{">=", "gt", "GT"} {
		if val, ok := q[i]; ok {
			return val, ok
		}
	}
	return nil, false
}

func isFilterLT(q map[string]any) (any, bool) {
	for _, i := range []string{"<=", "lt", "LT"} {
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

func (fb *FilterBuilder) ExtendGrip(q *gripql.Query, filterSelfName string) (*gripql.Query, error) {
	// isFilter filters out a top level "AND" that seems to be consistant across all queries in the exploration page
	if is_filter, ok := isFilter(fb.filter); ok {
		for _, array_filter := range is_filter.([]any) {
			if map_array_filter, ok := array_filter.(map[string]any); ok {
				fmt.Println("ARRAY FILTER: ", map_array_filter)
				if eq_arr_filter, ok := isFilterEQ(map_array_filter); ok {
					if map_eq_arr_filter, ok := eq_arr_filter.(map[string]any); ok {
						for filter_key, arr_filter_values := range map_eq_arr_filter {
							filter_key = fieldMap(filter_key)
							fmt.Println("FILTER KEY: ", filter_key, "ARR FILTER VALUES: ", arr_filter_values, "NAME: ", filterSelfName)
							if filterSelfName != "" && filter_key == filterSelfName {
								fmt.Println("_________________________________________________________________________")
								log.Infof("Filter Query %s", q.String())
								return q, nil
							} else {
								if filter_values, ok := arr_filter_values.([]any); ok {
									fmt.Println("FILTER VALUES: ", filter_values, "FILTER KEY: ", filter_key, "FILTER SELF NAME: ", filterSelfName)
									if len(filter_values) == 1 {
										q = q.Has(gripql.Within(filter_key, filter_values[0]))

									} else if len(filter_values) > 1 {
										final_expr := gripql.Or(gripql.Within(filter_key, filter_values[0]), gripql.Within(filter_key, filter_values[1]))
										for i := 2; i < len(filter_values); i++ {
											final_expr = gripql.Or(final_expr, gripql.Within(filter_key, filter_values[i]))
										}
										q = q.Has(final_expr)
									} else {
										log.Error("Error state filter not populated but list was created")
									}
								}
							}
						}
					}
				}
			}
		}
	}
	log.Infof("Filter Query %s", q.String())
	return q, nil
}
