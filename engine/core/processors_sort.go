package core

import (
	"context"
	"reflect"
	"slices"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// Sort rows
type Sort struct {
	sortFields []*gripql.SortField
}

// compareAny compares two variables of any type.
func compareAny(a, b any) int {
	// Get the types of the variables.
	ta := reflect.TypeOf(a)
	tb := reflect.TypeOf(b)

	// If the types are not the same, return a comparison based on type names.
	if ta != tb {
		return int(ta.Kind()) - int(tb.Kind())
	}

	// Compare values based on their types.
	switch ta.Kind() {
	case reflect.Int:
		// Compare integer values.
		return compareInts(a.(int), b.(int))
	case reflect.Float64:
		// Compare float values.
		return compareFloats(a.(float64), b.(float64))
	case reflect.String:
		// Compare string values.
		return compareStrings(a.(string), b.(string))
	case reflect.Bool:
		// Compare boolean values.
		return compareBooleans(a.(bool), b.(bool))
	case reflect.Struct:
		// Optionally handle structs here.
		// For now, just comparing based on memory address.
		return comparePointers(a, b)
	default:
		// For unsupported types, we just use pointers for comparison.
		log.Warningf("Unsupported types: %s %s", ta, tb)
		return comparePointers(a, b)
	}
}

// compareInts compares two integers.
func compareInts(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareFloats compares two float64 values.
func compareFloats(a, b float64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareStrings compares two string values.
func compareStrings(a, b string) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareBooleans compares two boolean values.
func compareBooleans(a, b bool) int {
	if !a && b {
		return -1
	} else if a && !b {
		return 1
	}
	return 0
}

// comparePointers compares two pointers.
func comparePointers(a, b interface{}) int {
	ptrA := reflect.ValueOf(a).Pointer()
	ptrB := reflect.ValueOf(b).Pointer()
	if ptrA < ptrB {
		return -1
	} else if ptrA > ptrB {
		return 1
	}
	return 0
}

func (s *Sort) compare(a, b gdbi.Traveler) int {
	for _, f := range s.sortFields {
		aVal := gdbi.TravelerPathLookup(a, f.Field)
		bVal := gdbi.TravelerPathLookup(b, f.Field)
		x := compareAny(aVal, bVal)
		if x != 0 {
			if f.Decending {
				return -x
			} else {
				return x
			}
		}
	}
	return 0
}

// Process runs LookupEdges
func (s *Sort) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	signals := []gdbi.Traveler{}

	list := []gdbi.Traveler{}

	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				signals = append(signals, t)
			} else {
				list = append(list, t) //TODO: develop disk backed system
			}
		}
		slices.SortFunc(list, s.compare)
		//emit signals first (?)
		for _, s := range signals {
			out <- s
		}
		for _, s := range list {
			out <- s
		}

	}()

	return ctx
}
