package gripql

// GetDataFieldTypes iterates over the data map and determines the type of each field
func GetDataFieldTypes(data map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for key, val := range data {
		if vMap, ok := val.(map[string]interface{}); ok {
			out[key] = GetDataFieldTypes(vMap)
			continue
		}
		if vSlice, ok := val.([]interface{}); ok {
			var vType interface{} = []interface{}{FieldType_UNKNOWN.String()}
			if len(vSlice) > 0 {
				vSliceVal := vSlice[0]
				if vSliceValMap, ok := vSliceVal.(map[string]interface{}); ok {
					vType = []map[string]interface{}{GetDataFieldTypes(vSliceValMap)}
				} else {
					vType = []interface{}{GetFieldType(vSliceVal)}
				}
			}
			out[key] = vType
			continue
		}
		out[key] = GetFieldType(val)
	}
	return out
}

// GetFieldType returns the FieldType for a value
func GetFieldType(field interface{}) string {
	switch field.(type) {
	case string:
		return FieldType_STRING.String()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return FieldType_NUMERIC.String()
	case float32, float64:
		return FieldType_NUMERIC.String()
	case bool:
		return FieldType_BOOL.String()
	default:
		return FieldType_UNKNOWN.String()
	}
}
