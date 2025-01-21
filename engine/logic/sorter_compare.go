package logic

import (
	"fmt"
	"reflect"

	"github.com/bmeg/grip/log"
)

func IsNumeric(a reflect.Type) bool {
	switch a.Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// CompareNumbers compares two numbers (of any compatible numeric type) and returns an int
// as expected by the sort package.
func CompareNumbers(a, b any) (int, error) {
	// Check the types using type switches
	switch x := a.(type) {
	case int:
		// Compare int with other types
		switch y := b.(type) {
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case int16:
		// Compare int16 with other types
		switch y := b.(type) {
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case int32:
		// Compare int32 with other types
		switch y := b.(type) {
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case int64:
		// Compare int64 with other types
		switch y := b.(type) {
		case int64:
			return CompareInts(x, y), nil
		case int:
			return CompareInts(x, int64(y)), nil
		case int16:
			return CompareInts(x, int64(y)), nil
		case int32:
			return CompareInts(x, int64(y)), nil
		case uint16:
			return CompareInts(x, int64(y)), nil
		case uint32:
			return CompareInts(x, int64(y)), nil
		case uint64:
			return CompareInts(x, int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case uint16:
		// Compare uint16 with other types
		switch y := b.(type) {
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case uint32:
		// Compare uint32 with other types
		switch y := b.(type) {
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case uint64:
		// Compare uint64 with other types
		switch y := b.(type) {
		case uint64:
			return CompareInts(int64(x), int64(y)), nil
		case int:
			return CompareInts(int64(x), int64(y)), nil
		case int16:
			return CompareInts(int64(x), int64(y)), nil
		case int32:
			return CompareInts(int64(x), int64(y)), nil
		case int64:
			return CompareInts(int64(x), y), nil
		case uint16:
			return CompareInts(int64(x), int64(y)), nil
		case uint32:
			return CompareInts(int64(x), int64(y)), nil
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		}
	case float32:
		// Compare float32 with other types
		switch y := b.(type) {
		case float32:
			return CompareFloats(float64(x), float64(y)), nil
		case float64:
			return CompareFloats(float64(x), y), nil
		case int:
			return CompareFloats(float64(x), float64(y)), nil
		case int16:
			return CompareFloats(float64(x), float64(y)), nil
		case int32:
			return CompareFloats(float64(x), float64(y)), nil
		case int64:
			return CompareFloats(float64(x), float64(y)), nil
		case uint16:
			return CompareFloats(float64(x), float64(y)), nil
		case uint32:
			return CompareFloats(float64(x), float64(y)), nil
		case uint64:
			return CompareFloats(float64(x), float64(y)), nil
		}
	case float64:
		// Compare float64 with other types
		switch y := b.(type) {
		case float64:
			return CompareFloats(x, y), nil
		case float32:
			return CompareFloats(x, float64(y)), nil
		case int:
			return CompareFloats(x, float64(y)), nil
		case int16:
			return CompareFloats(x, float64(y)), nil
		case int32:
			return CompareFloats(x, float64(y)), nil
		case int64:
			return CompareFloats(x, float64(y)), nil
		case uint16:
			return CompareFloats(x, float64(y)), nil
		case uint32:
			return CompareFloats(x, float64(y)), nil
		case uint64:
			return CompareFloats(x, float64(y)), nil
		}
	}

	// If we reach here, the types are not comparable
	return 0, fmt.Errorf("incompatible types: %s and %s", reflect.TypeOf(a).String(), reflect.TypeOf(b).String())
}

// CompareInts is a helper function to compare two int values and return the appropriate comparison result.
func CompareInts(x, y int64) int {
	if x < y {
		return -1
	} else if x > y {
		return 1
	}
	return 0
}

// CompareFloats is a helper function to compare two float values and return the appropriate comparison result.
func CompareFloats(x, y float64) int {
	if x < y {
		return -1
	} else if x > y {
		return 1
	}
	return 0
}

// CompareAny compares two variables of any type.
func CompareAny(a, b any) int {

	if a == nil && b != nil {
		return -1
	} else if a != nil && b == nil {
		return 1
	} else if a == nil && b == nil {
		return 0
	}

	// Get the types of the variables.
	ta := reflect.TypeOf(a)
	tb := reflect.TypeOf(b)

	// If the types are not the same, return a comparison based on type names.
	if ta != tb {
		if IsNumeric(ta) && IsNumeric(tb) {
			o, _ := CompareNumbers(a, b)
			return o
		}
		return int(ta.Kind()) - int(tb.Kind())
	}

	// Compare values based on their types.
	switch ta.Kind() {
	case reflect.Int:
		// Compare integer values.
		return CompareInts(int64(a.(int)), int64(b.(int)))
	case reflect.Float64:
		// Compare float values.
		return CompareFloats(a.(float64), b.(float64))
	case reflect.String:
		// Compare string values.
		return CompareStrings(a.(string), b.(string))
	case reflect.Bool:
		// Compare boolean values.
		return CompareBooleans(a.(bool), b.(bool))
	case reflect.Struct:
		// Optionally handle structs here.
		// For now, just comparing based on memory address.
		return ComparePointers(a, b)
	default:
		// For unsupported types, we just use pointers for comparison.
		log.Warningf("Unsupported types: %s %s", ta, tb)
		return ComparePointers(a, b)
	}
}

// CompareStrings compares two string values.
func CompareStrings(a, b string) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// CompareBooleans compares two boolean values.
func CompareBooleans(a, b bool) int {
	if !a && b {
		return -1
	} else if a && !b {
		return 1
	}
	return 0
}

// ComparePointers compares two pointers.
func ComparePointers(a, b interface{}) int {
	ptrA := reflect.ValueOf(a).Pointer()
	ptrB := reflect.ValueOf(b).Pointer()
	if ptrA < ptrB {
		return -1
	} else if ptrA > ptrB {
		return 1
	}
	return 0
}
