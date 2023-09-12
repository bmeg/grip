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
	// but it is consistant across exploration page queries
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
		fmt.Println("FILTER: ", is_filter)
		for _, array_filter := range is_filter.([]any) {
			// 'Checkbox' filter logic
			if map_array_filter, ok := array_filter.(map[string]any); ok {
				if mis_filter, ok := isFilterEQ(map_array_filter); ok {
					if map_eq_arr_filter, ok := mis_filter.(map[string]any); ok {
						for filter_key, arr_filter_values := range map_eq_arr_filter {
							filter_key = fieldMap(filter_key)
							if filter_values, ok := arr_filter_values.([]any); ok {

								// This is where the 'filterSelf' like Guppy parameter is implemented:
								// If the current property that is being passed into the filter function
								// is the same as the current interated key then return early and skip filtering self
								if filterSelfName != "" && filter_key == filterSelfName {
									log.Infof("FilterSelf Query Condition Hit %s", q.String())
									return q, nil

									// otherwise split filtering by 1 checked box or multiple checked boxes
									// build the query with ORs like it is done in the current data portal
								} else if len(filter_values) == 1 {
									q = q.Has(gripql.Within(filter_key, filter_values[0]))

								} else if len(filter_values) > 1 {
									final_expr := gripql.Or(gripql.Within(filter_key, filter_values[0]), gripql.Within(filter_key, filter_values[1]))
									for i := 2; i < len(filter_values); i++ {
										final_expr = gripql.Or(final_expr, gripql.Within(filter_key, filter_values[i]))
									}
									q = q.Has(final_expr)
								} else {
									log.Error("Error state checkbox filter not populated but list was created")
								}

							}

						}
					}
				}

			}
			// 'Slider' filter logic. Don't think filter self is needed
			// for slider since it accepts a range of values
			if map_array_filter, ok := array_filter.(map[string]any); ok {
				if is_filter, ok := isFilter(map_array_filter); ok {
					if map_eq_arr_filter, ok := is_filter.([]any); ok {
						for _, v := range map_eq_arr_filter {
							if map_array_filter, ok := v.(map[string]any); ok {
								if val, ok := isFilterGT(map_array_filter); ok {
									if vMap, ok := val.(map[string]any); ok {
										for k, v := range vMap {
											k = fieldMap(k)
											q = q.Has(gripql.Gt(k, v))
										}
									}
								}

								if val, ok := isFilterLT(map_array_filter); ok {
									if vMap, ok := val.(map[string]any); ok {
										for k, v := range vMap {
											k = fieldMap(k)
											q = q.Has(gripql.Lt(k, v))
										}
									}
								}

							} else {
								log.Error("Error state slider filter not populated but list was created")
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
