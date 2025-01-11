package logic

import (
	"strconv"
)

func ConvertToType(input any, targetType string) any {
	switch targetType {
	case "string":
		switch v := input.(type) {
		case string:
			return v
		case int:
			return strconv.Itoa(v)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(v)
		default:
			return ""
		}
	case "bool":
		switch v := input.(type) {
		case string:
			parsedBool, _ := strconv.ParseBool(v)
			return parsedBool
		case int:
			return v != 0
		case float64:
			return v != 0.0
		case bool:
			return v
		default:
			return false
		}
	case "float":
		switch v := input.(type) {
		case string:
			parsedFloat, _ := strconv.ParseFloat(v, 64)
			return parsedFloat
		case int:
			return float64(v)
		case float64:
			return v
		case bool:
			if v {
				return 1.0
			}
			return 0.0
		default:
			return 0.0
		}
	case "int":
		switch v := input.(type) {
		case string:
			parsedInt, _ := strconv.Atoi(v)
			return parsedInt
		case int:
			return v
		case float64:
			return int(v)
		case bool:
			if v {
				return 1
			}
			return 0
		default:
			return 0
		}
	case "list":
		switch v := input.(type) {
		case []any:
			return v
		default:
			return []any{v}
		}
	default:
		return nil
	}
}
